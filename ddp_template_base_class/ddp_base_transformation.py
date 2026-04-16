import json
import os
import traceback
from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    datediff,
    dense_rank,
    desc,
    greatest,
    lit,
    max,
    when,
)
from sap_bdc_fos_utils.enums import (
    DPJobStatusEnum,
    DPLifecyclePhaseEnum,
    DPStatusMessageTypeEnum,
)
from sap_bdc_fos_utils.dp_uri import TableURIBuilder
from sap_bdc_fos_utils.dp_uri_v2 import TableURIBuilderV2
from sap_bdc_fos_utils.registry import TenantShareRegistry, TenantTableRegistry
from sap_bdc_fos_utils import (
    CSNInteropRoot,
    DPDataLakeServiceClient,
    DPDynamicShareUtil,
    DPLogger,
    DPSparkSession,
    FosAppArgsUtil,
    StatusMessage,
    utils,
)
from sap_bdc_fos_utils.csn_interop import add_schema_metadata
from bdc_ia_ddproducts.fos.fos_utilities import FosDataLoadUtil
from bdc_ia_ddproducts.fos.fos_constants import FosDataLoadConstants
from bdc_ia_ddproducts.fos.fos_status_utilities import FosStatusUtil
from sap_bdc_fos_utils.enums import (
    DPDataLayerEnum,
)


class BaseTransformationJob(ABC):
    def __init__(self, spark, logger, context, dp_spark):
        self.spark = spark
        self.logger = logger
        self.context = context
        self.dp_spark = dp_spark
        self.context.spark = spark
        self.context.dp_spark = self.dp_spark
        self.fos_data_load_util = FosDataLoadUtil(self.spark, self.logger)
        self.dp_dynamic_share_util = DPDynamicShareUtil(self.spark)
        # self.dp_delta = DPDataLakeServiceClient(self.spark).delta_table_uri_builder()
        self.fos_status_util = FosStatusUtil(self.spark, self.logger, self.context)

    def retrieve_csn(
        self, dpd_dict: Dict[str, Tuple[Dict[str, str], str]], key: str
    ) -> str:
        value = dpd_dict.get(key)
        if value is not None and isinstance(value, tuple) and len(value) == 2:
            json_str = value[1]
            if json_str:
                return json_str
            else:
                raise KeyError(f"Missing CSN data for key '{key}' in the dictionary.")
        else:
            raise KeyError(
                f"The key '{key}' not found in the dictionary, or its value has an unexpected structure."
            )

    def retrieve_target_path(self, dpd_dict, key, dp_delta, tenant_id):
        value = dpd_dict.get(key)
        if value is not None and isinstance(value, tuple) and len(value) == 2:
            coordinates_dict = value[0]
            if coordinates_dict and isinstance(coordinates_dict, dict):
                layer = coordinates_dict.get("layer")
                namespace = coordinates_dict.get("namespace")
                version = coordinates_dict.get("version")
                name = coordinates_dict.get("name")
                return self.fos_data_load_util.get_delta_path(
                    dp_delta, tenant_id, layer, version, namespace, name
                )
            else:
                raise KeyError(f"Missing coordinate data for key '{key}' in dpd_dict.")
        else:
            raise KeyError(
                f"The key '{key}' not found in the dictionary, or its value has an unexpected structure."
            )

    # ==== STANDARD DATA ACCESS METHODS ====
    def load_table(self, table_name: str) -> DataFrame:

        _dict = self.current_dependent_tables_dict
        table_info = _dict[table_name][0]
        table_path = table_info["location"]
        self.logger.info(f"Loading table {table_name} from location: {table_path}")
        df = self.spark.read.format("delta").load(table_path)

        # Get record count efficiently using DataFrame.count()
        # This triggers a Spark job but is the most efficient way to count all records
        record_count = df.count()
        self.logger.info(f"Loaded {record_count:,} records from table {table_name}")

        return df

    def write_delta_table(
        self, df: DataFrame, table_path: str, enable_cdf: bool = True
    ):
        """
        Standard Delta table writing with optimal settings.
        """
        df.write.format("delta").mode("overwrite").save(table_path)
        if enable_cdf:
            sql_command = f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            self.context.spark.sql(sql_command)

    def get_result_table_path(self, derived_data_product_coords: Dict[str, Any]) -> str:

        dp_delta = DPDataLakeServiceClient(self.spark).delta_table_uri_builder(
            self.context.tenant_id, derived_data_product_coords
        )

        # Get layer from coordinates or use default
        layer = derived_data_product_coords.get("layer", "gold")
        name = derived_data_product_coords.get("name")

        if not name:
            raise ValueError("Table name is required in coordinates")

        # Build path using fluent interface with version check
        if isinstance(dp_delta, TableURIBuilder):
            self.logger.info("Using TableURIBuilder (V1) to resolve table path.")
            version = derived_data_product_coords.get("version")
            namespace = derived_data_product_coords.get("namespace")
            path = self.fos_data_load_util.get_delta_path(
                dp_delta,
                self.context.tenant_id,
                layer,
                version,
                namespace,
                name,
            )
        elif isinstance(dp_delta, TableURIBuilderV2):
            self.logger.info("Using TableURIBuilderV2 (V2) to build table path.")
            path = (
                dp_delta.medallion_layer(layer)
                .table_name(name)
                .build()
                # .name(name)
            )
        else:
            raise TypeError(f"Unsupported URI builder type: {type(dp_delta)}")

        self.logger.info(f"Building result table path for '{name}' in layer '{layer}'")
        return path

    def get_spark(self):
        """Return the SparkSession object."""
        return self.spark

    def get_dependent_tables_from_share_registry(self, share_id_list, tenant_id):
        dependent_tables_dict = {}
        tenant_share_registry = TenantShareRegistry(self.context.dp_spark, tenant_id)
        for share_id in share_id_list:
            self.logger.info(f"Begin processing for dependent share_id: {share_id}")
            share_registry_tables_info = tenant_share_registry.get_share_tables(
                share_id
            )
            if not share_registry_tables_info:
                self.logger.warning(
                    f"No table information found for share id: {share_id}"
                )
                return None, False
            for key, value in share_registry_tables_info.items():
                if key in dependent_tables_dict:
                    if isinstance(dependent_tables_dict[key], list) and isinstance(
                        value, list
                    ):
                        dependent_tables_dict[key].extend(value)
                    else:
                        dependent_tables_dict[key] = value
                else:
                    dependent_tables_dict[key] = value
        return dependent_tables_dict, True

    def validate_dependencies(
        self, dependent_tables_dict, expected_tables_list, tenant_id
    ):
        if not isinstance(dependent_tables_dict, dict):
            raise TypeError("Dependent tables is not a dictionary.")
        if not isinstance(expected_tables_list, list):
            raise TypeError("Expected tables list is not a list.")
        dep_tables = dependent_tables_dict.keys()
        for expected_table in expected_tables_list:
            if expected_table not in dep_tables:
                self.logger.warning(
                    f"Missing expected table: `{expected_table}` in the dependent table list."
                )
                return False
            table_path = dependent_tables_dict[expected_table][0]["location"]
            if not utils.is_delta_table(self.spark, table_path):
                self.logger.warning(
                    f"Table {expected_table} in the tenant table registry is not a delta table."
                )
                return False
        return True

    def pipeline(self, app_args):
        self.logger.info("--- BEGIN pipeline input arguments ---")
        (
            transformer_config,
            tenants_config,
            hdlfs_config,
            data_products,
            datasets,
            data_sources,
            shares,
            lcm_info,
            derived_data_product_to_its_properties_dict,
            derived_data_product_to_its_coordinates_dict,
            share_ids,
        ) = self.extract_application_parameters(app_args)
        self.context.tenant_agnostic_hdlfs_path = hdlfs_config[
            FosDataLoadConstants.SHARED
        ][FosDataLoadConstants.TENANT_AGNOSTIC_CONTAINER][
            FosDataLoadConstants.HDLFS_CONTAINER_PATH
        ]

        for tenant_id in tenants_config.keys():
            self.logger.info(
                f"Begin data transformation processing for tenant: {tenant_id}"
            )
            self.context.tenant_id = tenant_id
            for (
                derived_data_product,
                derived_dataproduct_properties,
            ) in derived_data_product_to_its_properties_dict.items():
                try:
                    self.logger.info(
                        f"Begin data transformation processing for derived data product: {derived_data_product}"
                    )
                    self.dp_delta = DPDataLakeServiceClient(
                        self.spark
                    ).delta_table_uri_builder(
                        tenant_id,
                        derived_data_product_to_its_coordinates_dict[
                            derived_data_product
                        ],
                    )
                    self.context.dataproduct_id = derived_data_product
                    self.context.correlation_ids = lcm_info.get(
                        self.context.dataproduct_id, {}
                    ).get("correlationId", None)
                    # Set status to 'active'
                    status_msg = StatusMessage(
                        DPStatusMessageTypeEnum.INFO,
                        FosDataLoadConstants.JOB_STATUS_INFO,
                        "active",
                    )
                    self.fos_status_util.set_job_status(
                        status_msg,
                        self.context.dataproduct_id,
                        self.context.correlation_ids,
                    )
                    transformer_parameters_dict, derived_data_product_metadata = (
                        self.extract_transformer_parameters(
                            derived_dataproduct_properties, transformer_config
                        )
                    )
                    share_ids_list = self.extract_share_ids(
                        derived_dataproduct_properties
                    )
                    dependent_tables_dict, found_dpd_artifacts = (
                        self.get_dependent_tables_from_share_registry(
                            share_ids_list, tenant_id
                        )
                    )
                    if not found_dpd_artifacts:
                        self.logger.warning(
                            f"Not all shares in the input port of {derived_data_product} are created yet. Skipping transformation for this derived data product."
                        )
                        continue

                    # Store dependent_tables_dict for use by load_table method
                    self.current_dependent_tables_dict = dependent_tables_dict

                    if dependent_tables_dict:
                        self.logger.info(
                            f"Dependent tables for derived data product {list(dependent_tables_dict.keys())}:"
                        )
                    else:
                        self.logger.warning("No dependent tables found")
                    expected_dependencies = transformer_parameters_dict.get(
                        "input_tables", None
                    )
                    if expected_dependencies:
                        expected_dependencies = expected_dependencies.split(",")
                        if not self.validate_dependencies(
                            dependent_tables_dict, expected_dependencies, tenant_id
                        ):
                            self.logger.warning(
                                f"Not all expected input share tables found for dataproduct: {derived_data_product} yet. Skipping the transformation for this derived_data_product."
                            )
                            continue
                    else:
                        self.logger.warning(
                            "input_tables not provided as part of transformation parameter. Skipping to validate dependencies"
                        )

                    result_df = self.run_transformation(
                        dependent_tables_dict, tenant_id, transformer_parameters_dict
                    )
                    self.register_tables(
                        result_df,
                        derived_data_product_metadata,
                        hdlfs_config,
                        dependent_tables_dict,
                        derived_data_product_to_its_coordinates_dict,
                        derived_data_product,
                        tenant_id,
                        transformer_parameters_dict,  # Pass external config to register_tables
                    )
                    self.logger.info(
                        f"Table registration successful for: {derived_data_product}"
                    )
                    status_msg = StatusMessage(
                        DPStatusMessageTypeEnum.INFO,
                        FosDataLoadConstants.JOB_STATUS_INFO,
                        FosDataLoadConstants.SUCCESS,
                    )
                except Exception as e:
                    status_msg = StatusMessage(
                        DPStatusMessageTypeEnum.ERROR,
                        FosDataLoadConstants.JOB_STATUS_ERROR,
                        FosDataLoadConstants.ERROR,
                    )
                    self.logger.error(
                        f"Error during data transformation for derived dataproduct {derived_data_product}: {e}"
                    )
                    raise e
                finally:
                    self.logger.info(f"Setting job status: {status_msg}")
                    self.fos_status_util.set_job_status(
                        status_msg,
                        self.context.dataproduct_id,
                        self.context.correlation_ids,
                    )

    @abstractmethod
    def run_transformation(
        self, dependent_tables_dict, tenant_id, transformer_parameters_dict
    ):
        pass

    # ==== FRAMEWORK STANDARD REGISTRATION ====
    def register_tables_standard(
        self,
        result_df,
        derived_data_product_metadata,
        hdlfs_config,
        dependent_tables_dict,
        derived_data_product_to_its_coordinates_dict,
        derived_data_product,
        tenant_id,
        transformer_parameters_dict,  # External configuration
    ):
        """
        Standard table registration process using external configuration.
        Teams don't need to override this - configuration comes from external parameters.
        """
        # Use external configuration from transformer_parameters_dict instead of hardcoded config

        # Prepare CSN info using standard process
        csn_info = self._prepare_csn_info_standard(
            derived_data_product_metadata,
            hdlfs_config,
            dependent_tables_dict,
            derived_data_product_to_its_coordinates_dict,
            derived_data_product,
            result_df,
        )

        # Write and register using standard process with external config
        self._write_and_register_table_standard(
            csn_info,
            tenant_id,
            derived_data_product_to_its_coordinates_dict,
            derived_data_product,
            transformer_parameters_dict,  # External config instead of hardcoded
        )

        self.logger.info(
            f"Table registration successful for: {csn_info['table_path']} under: {derived_data_product_to_its_coordinates_dict[derived_data_product]}"
        )

    @abstractmethod
    def register_tables(
        self,
        result_df,
        derived_data_product_metadata,
        hdlfs_config,
        dependent_tables_dict,
        derived_data_product_to_its_coordinates_dict,
        derived_data_product,
        tenant_id,
        transformer_parameters_dict,  # External configuration
    ):
        """
        Teams can override this if they need custom registration logic,
        or delegate to register_tables_standard() for standard behavior.
        External configuration is passed via transformer_parameters_dict.
        """
        pass

    def extract_transformer_parameters(
        self, derived_dataproduct_properties, transformer_config
    ):
        """Extract transformer parameters and metadata for a derived data product using FosAppArgsUtil."""
        transformer_name = transformer_config.get("transformerId")
        derived_data_product_transformers_list = (
            FosAppArgsUtil.extract_transformers_from_derived_data_product_properties(
                derived_dataproduct_properties
            )
        )
        derived_data_product_metadata = (
            FosAppArgsUtil.extract_metadata_from_derived_data_product_properties(
                derived_dataproduct_properties
            )
        )
        transformer_parameters = [
            param
            for transformer in derived_data_product_transformers_list
            if transformer.get("transformerName") == transformer_name
            for param in transformer.get("parameters", [])
        ]
        transformer_parameters_dict = dict(
            param.split("=") for param in transformer_parameters
        )
        return transformer_parameters_dict, derived_data_product_metadata

    def extract_application_parameters(self, app_args):
        """Extract and return all required application parameters from app_args using FosAppArgsUtil."""
        transformer_config = FosAppArgsUtil.extract_fos_transformer_config(app_args)
        tenants_config = FosAppArgsUtil.extract_fos_tenants_config(app_args)
        hdlfs_config = FosAppArgsUtil.extract_hdlfs_config(app_args)
        data_products = FosAppArgsUtil.extract_data_products(app_args)
        datasets = FosAppArgsUtil.extract_datasets(app_args)
        data_sources = FosAppArgsUtil.extract_data_sources(app_args)
        shares = FosAppArgsUtil.extract_fos_shares(app_args)
        lcm_info = FosAppArgsUtil.extract_fos_lcm_info(app_args)
        derived_data_product_to_its_properties_dict = (
            FosAppArgsUtil.extract_derived_data_product_properties(data_products)
        )
        derived_data_product_to_its_coordinates_dict = (
            FosAppArgsUtil.extract_dpd_coordinates(data_products)
        )
        share_ids = FosAppArgsUtil.extract_share_id(data_products)
        return (
            transformer_config,
            tenants_config,
            hdlfs_config,
            data_products,
            datasets,
            data_sources,
            shares,
            lcm_info,
            derived_data_product_to_its_properties_dict,
            derived_data_product_to_its_coordinates_dict,
            share_ids,
        )

    def _prepare_csn_info_standard(
        self,
        derived_data_product_metadata,
        hdlfs_config,
        dependent_tables_dict,
        derived_data_product_to_its_coordinates_dict,
        derived_data_product,
        result_df,
    ):
        """
        Standard CSN preparation process - same logic for all transformers.
        """
        csn_document_json_filename = (
            FosAppArgsUtil.get_tenant_agnostic_csn_json_filename(
                derived_data_product_metadata
            )
        )
        csn_document_json_location = FosAppArgsUtil.get_csn_document_json_location(
            csn_document_json_filename
        )
        tenant_agnostic_hdlfs_path = FosAppArgsUtil.get_tenant_agnostic_hdlfs_path(
            hdlfs_config
        )
        tenant_agnostic_csn_dict = self.fos_data_load_util.read_json(
            tenant_agnostic_hdlfs_path, csn_document_json_location
        )
        tenant_agnostic_csn = CSNInteropRoot.from_dict(tenant_agnostic_csn_dict)
        json_csn = tenant_agnostic_csn.to_json()
        self.matched_derived_data_product_entity_name = (
            tenant_agnostic_csn.retrieve_matching_entity_name_from_csn(
                derived_data_product_to_its_coordinates_dict[derived_data_product][
                    "name"
                ]
            )
        )
        df = tenant_agnostic_csn.map_entity_to_dataframe(
            self.matched_derived_data_product_entity_name, result_df
        )

        table_path = self.get_result_table_path(
            derived_data_product_to_its_coordinates_dict[derived_data_product]
        )

        return {
            "df": df,
            "json_csn": json_csn,
            "table_path": table_path,
            "matched_derived_data_product_entity_name": self.matched_derived_data_product_entity_name,
            "namespace": derived_data_product_to_its_coordinates_dict[
                derived_data_product
            ]["namespace"],
            "version": derived_data_product_to_its_coordinates_dict[
                derived_data_product
            ]["version"],
        }

    def _write_and_register_table_standard(
        self,
        csn_info,
        tenant_id,
        derived_data_product_to_its_coordinates_dict,
        derived_data_product,
        transformer_parameters_dict,  # External config instead of hardcoded config
    ):
        """
        Standard table writing and registration process using external configuration.
        """
        df = csn_info["df"]
        table_path = csn_info["table_path"]
        json_csn = csn_info["json_csn"]
        self.matched_derived_data_product_entity_name = csn_info[
            "matched_derived_data_product_entity_name"
        ]

        # Use external configuration from transformer_parameters_dict
        enable_cdf = (
            transformer_parameters_dict.get("enable_cdf", "true").lower() == "true"
        )
        self.write_delta_table(df, table_path, enable_cdf)
        table_args = TenantTableRegistry.TableArgs(
            layer=DPDataLayerEnum.GOLD,
            table_name=self.matched_derived_data_product_entity_name,
            csn=json_csn,
        )
        # Register in tenant table registry
        tenant_table_registry = TenantTableRegistry(self.context.dp_spark, tenant_id)
        tenant_table_registry.register(
            derived_data_product_to_its_coordinates_dict[derived_data_product],
            [table_args],
        )

    @classmethod
    def main(cls, transformation_cls):
        """Standardized main entrypoint for all transformation jobs."""
        dp_logger = None
        try:
            dp_spark = DPSparkSession()
            context = utils.ClassBuilder()
            spark_config_key = os.environ.get(
                FosDataLoadConstants.SPARK_CONFIG_KEY,
                FosDataLoadConstants.DEFAULT_SPARK_CONFIG_KEY,
            )
            spark = dp_spark.get_spark(spark_config_key)
            dp_logger = DPLogger(spark)
            transformation = transformation_cls(spark, dp_logger, context, dp_spark)
            app_args = dp_spark.get_app_args()
            transformation.pipeline(app_args)
            dp_logger.info("TransformationJob is successful")
        except Exception as e:
            if dp_logger:
                dp_logger.error(f"TransformationJob failed: {traceback.format_exc()}")
            else:
                raise e
        finally:
            if dp_logger:
                dp_logger.info("TransformationJob completed")

    def extract_share_ids(self, derived_dataproduct_properties):
        """Extract share IDs from derived data product properties using FosAppArgsUtil."""
        input_ports = FosAppArgsUtil.extract_input_ports_from_properties(
            derived_dataproduct_properties
        )
        return FosAppArgsUtil.extract_all_shareids_from_input_ports(input_ports)
