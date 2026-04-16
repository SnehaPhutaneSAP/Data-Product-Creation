import json
from typing import Any, Dict, Tuple , List, Dict, List, Tuple, Optional
    
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
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
from sap_bdc_fos_utils.registry import TenantShareRegistry, TenantTableRegistry
import os
import traceback
from abc import ABC, abstractmethod
from sap_bdc_fos_utils import (
    DPDataLakeServiceClient,
    DPDynamicShareUtil,
    DPLogger,
    DPSparkSession,
    FosAppArgsUtil,
    StatusMessage,
    utils,
)
from ddp_template.fos.fos_utilities import FosDataLoadUtil
from sap_bdc_fos_utils import CSNInteropRoot
from sap_bdc_fos_utils.csn_interop import add_schema_metadata
from ddp_template.fos.fos_status_utilities import FosStatusUtil
from ddp_template.fos.fos_constants import FosDataLoadConstants

class BaseTransformationJob(ABC):
    def __init__(self, spark, logger, context, dp_spark,
        is_dataset_pipeline: bool = True,
        is_dataproduct_pipeline: bool = False,
        is_datasource_pipeline: bool = False,
        pre_execution_handler: Optional[Callable[..., None]] = None,
        post_execution_handler: Optional[Callable[..., None]] = None,
        exception_handler: Optional[Callable[..., None]] = None):
        self.spark = spark
        self.logger = logger
        self.context = context
        self.dp_spark = dp_spark
        self.fos_data_load_util = FosDataLoadUtil(self.spark, self.logger)
        self.dp_dynamic_share_util = DPDynamicShareUtil(self.spark)
        self.dp_delta = DPDataLakeServiceClient(self.spark).delta_table_uri_builder()
        self.fos_status_util = FosStatusUtil(self.spark, self.logger, self.context)

        flags = [is_dataset_pipeline, is_dataproduct_pipeline, is_datasource_pipeline]
        if sum(flags) > 1:
            raise ValueError("Only one of 'is_dataset_pipeline', 'is_dataproduct_pipeline', or 'is_datasource_pipeline' can be True.")
        self.is_dataset_pipeline = is_dataset_pipeline
        self.is_dataproduct_pipeline = is_dataproduct_pipeline
        self.is_datasource_pipeline = is_datasource_pipeline
        self.pre_execution_handler_method = pre_execution_handler or self.default_pre_execution_handler
        self.post_execution_handler_method = post_execution_handler or self.default_post_execution_handler
        self.exception_handler_method = exception_handler or self.default_exception_handler

    def default_pre_execution_handler(self, *args, **kwargs) -> None:
        self.logger.info("Default pre-execution handler for the transformation engine.")

    def default_post_execution_handler(self, *args, **kwargs) -> None:
        self.logger.info("Default post-execution handler for the transformation engine.")

    def default_exception_handler(self, *args, **kwargs) -> None:
        self.logger.error("Default exception handler for the transformation engine.")

    def retrieve_csn(self, dpd_dict: Dict[str, Tuple[Dict[str, str], str]], key: str) -> str:
        value = dpd_dict.get(key)
        if value is not None and isinstance(value, tuple) and len(value) == 2:
            json_str = value[1]
            if json_str:
                return json_str
            else:
                raise KeyError(f"Missing CSN data for key '{key}' in the dictionary.")
        else:
            raise KeyError(f"The key '{key}' not found in the dictionary, or its value has an unexpected structure.")

    def get_delta_path(self, dp_delta, tenant_id, layer, version, namespace, name):
        return (
            dp_delta.tenant_id(tenant_id)
            .medallion_layer(layer)
            .version(version)
            .namespace(namespace)
            .name(name)
            .build()
        )

    def retrieve_target_path(self, dpd_dict, key, dp_delta, tenant_id):
        value = dpd_dict.get(key)
        if value is not None and isinstance(value, tuple) and len(value) == 2:
            coordinates_dict = value[0]
            if coordinates_dict and isinstance(coordinates_dict, dict):
                return self.get_delta_path(dp_delta, tenant_id, **coordinates_dict)
            else:
                raise KeyError(f"Missing coordinate data for key '{key}' in dpd_dict.")
        else:
            raise KeyError(f"The key '{key}' not found in the dictionary, or its value has an unexpected structure.")

    def __sanitize_version(self, input_dict):
        if "version" in input_dict and input_dict["version"].startswith("v1.0.0"):
            input_dict["version"] = input_dict["version"][1:]
        return input_dict

    def retrieve_tables_from_tenant_table_registry(self, context, dpd_dict: Dict[str, str]) -> Dict[str, Tuple[Dict[str, str], str]]:
        tenant_table_registry = TenantTableRegistry(context.dp_spark, context.tenant_id)
        try:
            return tenant_table_registry.retrieve_tables(self.__sanitize_version(dpd_dict))
        except Exception:
            return {}

    def get_dependent_tables_from_share_registry(self, share_id_list, tenant_id):
        dependent_tables_dict = {}
        tenant_share_registry = TenantShareRegistry(self.context.dp_spark, tenant_id)
        for share_id in share_id_list:
            self.logger.info(f"Begin processing for dependent share_id: {share_id}")
            share_registry_tables_info = tenant_share_registry.get_share_tables(share_id)
            if not share_registry_tables_info:
                self.logger.warning(f"No table information found for share id: {share_id}")
                return None, False
            for key, value in share_registry_tables_info.items():
                if key in dependent_tables_dict:
                    if isinstance(dependent_tables_dict[key], list) and isinstance(value, list):
                        dependent_tables_dict[key].extend(value)
                    else:
                        dependent_tables_dict[key] = value
                else:
                    dependent_tables_dict[key] = value
        return dependent_tables_dict, True

    def get_dependent_tables(self, dpd_dict_list, context):
        dependent_tables_dict = {}
        for dpd_dict in dpd_dict_list:
            context.logger.info(f"Begin processing for dependent dpd_artifact: {dpd_dict}")
            table_registry_tables_info = self.retrieve_tables_from_tenant_table_registry(context, dpd_dict)

            if not table_registry_tables_info:
                context.logger.warning(f"No table registry information found for dpd_dict: {dpd_dict}")
                return None, False

            for key, value in table_registry_tables_info.items():
                if key in dependent_tables_dict:
                    if isinstance(dependent_tables_dict[key], list) and isinstance(value, list):
                        dependent_tables_dict[key].extend(value)
                    else:
                        dependent_tables_dict[key] = value
                else:
                    dependent_tables_dict[key] = value

        return dependent_tables_dict, True

    def validate_dependencies(self, ttr_dependent_tables_dict, expected_tables_list, tenant_id):
        if not isinstance(ttr_dependent_tables_dict, dict):
            raise TypeError("Dependent tables is not a dictionary.")
        if not isinstance(expected_tables_list, list):
            raise TypeError("Expected tables list is not a list.")

        dep_tables = ttr_dependent_tables_dict.keys()
        for expected_table in expected_tables_list:
            if expected_table not in dep_tables:
                self.logger.warning(f"Missing expected table: `{expected_table}` in the dependent table list.")
                return False
            table_path = ttr_dependent_tables_dict[expected_table][0]["location"]
            if not utils.is_delta_table(self.spark, table_path):
                self.logger.warning(f"Table {expected_table} in the tenant table registry is not a delta table.")
                return False

        return True

    def pipeline(self, app_args):
        self.logger.info("--- BEGIN pipeline input arguments ---")
        transformer_config, tenants_config, hdlfs_config, data_products, datasets, data_sources, shares, lcm_info, derived_data_product_to_its_properties_dict, derived_data_product_to_its_coordinates_dict, share_ids = self.extract_application_parameters(app_args)
        self.context.tenant_agnostic_hdlfs_path = hdlfs_config[FosDataLoadConstants.SHARED][FosDataLoadConstants.TENANT_AGNOSTIC_CONTAINER][FosDataLoadConstants.HDLFS_CONTAINER_PATH]
        for tenant_id in tenants_config.keys():
            self.logger.info(f"Begin data transformation processing for tenant: {tenant_id}")
            self.context.tenant_id = tenant_id
            for derived_data_product, derived_dataproduct_properties in derived_data_product_to_its_properties_dict.items():
                try:
                    self.logger.info(f"Begin data transformation processing for derived data product: {derived_data_product}")
                    self.context.dataproduct_id = derived_data_product
                    self.context.correlation_ids = lcm_info.get(self.context.dataproduct_id, {}).get("correlationId", None)
                    transformer_parameters_dict, derived_data_product_metadata = self.extract_transformer_parameters(derived_dataproduct_properties, transformer_config)
                    share_ids_list = self.extract_share_ids(derived_dataproduct_properties)
                    dependent_tables_dict, found_dpd_artifacts = self.get_dependent_tables(share_ids_list, tenant_id)
                    if not found_dpd_artifacts:
                        self.logger.warning(f"Not all shares in the input port of {derived_data_product} are created yet. Skipping transformation for this derived data product.")
                        continue
                    self.logger.info(f"Dependent tables for derived data product {dependent_tables_dict.keys()}:")
                    expected_dependencies = transformer_parameters_dict.get("input_tables", None)

                    if expected_dependencies:
                        expected_dependencies = expected_dependencies.split(",")
                        if not self.validate_dependencies(dependent_tables_dict, expected_dependencies, tenant_id):
                            self.logger.warning(f"Not all expected input share tables found for dataproduct: {derived_data_product} yet. Skipping the transformation for this derived_data_product.")
                            continue
                    else:
                        self.logger.warning("input_tables not provided as part of transformation parameter. Skipping to validate dependencies")
                    result_df = self.run_transformation(dependent_tables_dict, tenant_id, transformer_parameters_dict)
                    self.register_tables(result_df, derived_data_product_metadata, hdlfs_config, dependent_tables_dict, derived_data_product_to_its_coordinates_dict, derived_data_product, tenant_id)
                    self.logger.info(f"Table registration successful for: {derived_data_product}")
                    status_msg = StatusMessage(DPStatusMessageTypeEnum.INFO, FosDataLoadConstants.JOB_STATUS_INFO, FosDataLoadConstants.SUCCESS)
                except Exception as e:
                    status_msg = StatusMessage(DPStatusMessageTypeEnum.ERROR, FosDataLoadConstants.JOB_STATUS_ERROR, FosDataLoadConstants.ERROR)
                    self.logger.error(f"Error during data transformation for derived dataproduct {derived_data_product}: {e}")
                    raise e
                finally:
                    self.logger.info(f"Setting job status: {status_msg}")
                    self.fos_status_util.set_job_status(status_msg, self.context.dataproduct_id, self.context.correlation_ids)

    @abstractmethod
    def run_transformation(self, dependent_tables_dict, tenant_id, transformer_parameters_dict):
        pass

    @abstractmethod
    def register_tables(self, result_df, derived_data_product_metadata, hdlfs_config, dependent_tables_dict, derived_data_product_to_its_coordinates_dict, derived_data_product, tenant_id):
        pass

    def extract_transformer_parameters(self, derived_dataproduct_properties, transformer_config):
        transformer_name = transformer_config.get("transformerId")
        derived_data_product_transformers_list = FosAppArgsUtil.extract_transformers_from_derived_data_product_properties(
            derived_dataproduct_properties
        )
        derived_data_product_metadata = FosAppArgsUtil.extract_metadata_from_derived_data_product_properties(
            derived_dataproduct_properties
        )
        transformer_parameters = [
            param for transformer in derived_data_product_transformers_list
            if transformer.get("transformerName") == transformer_name
            for param in transformer.get("parameters", [])
        ]
        transformer_parameters_dict = dict(param.split("=") for param in transformer_parameters)
        return transformer_parameters_dict, derived_data_product_metadata

    def extract_application_parameters(self, app_args):
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

    @classmethod
    def main(cls, transformation_cls):
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
            import traceback
            if dp_logger:
                dp_logger.error(f"TransformationJob failed: {traceback.format_exc()}")
            else:
                raise e
        finally:
            if dp_logger:
                dp_logger.info("TransformationJob completed")

    def extract_share_ids(self, derived_dataproduct_properties):
        input_ports = FosAppArgsUtil.extract_input_ports_from_properties(derived_dataproduct_properties)
        return FosAppArgsUtil.extract_all_shareids_from_input_ports(input_ports)