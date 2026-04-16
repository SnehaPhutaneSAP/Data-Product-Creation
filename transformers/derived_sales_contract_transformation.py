"""This module contains the ProcodeExample Transformation job class which is responsible for showcasing procode transformation on a derived data product."""

from bdc_ia_ddproducts.ddp_template_base_class.ddp_base_transformation import BaseTransformationJob
from sap_bdc_fos_utils import FosAppArgsUtil, CSNInteropRoot, DPDataLakeServiceClient, StatusMessage
from sap_bdc_fos_utils.registry import TenantTableRegistry

class ProcodeDdpTemplate_transformationJob(BaseTransformationJob):
    """Transformer to build a customer order experiences data product."""

    def _load_all_delta_tables(self, dependent_tables_dict, tenant_id):
        """Load all required delta tables for the transformation. Override in subclass for custom logic."""
        sales_contract_df = self._load_delta_table(dependent_tables_dict, "silver:sap.s4com:SalesContract:1.0.0", tenant_id)
        # Select only the fields defined in DerivedSalesContract CSN
        return sales_contract_df

    def _prepare_csn_info(self, derived_data_product_metadata, hdlfs_config, dependent_tables_dict, derived_data_product_to_its_coordinates_dict, derived_data_product, result_df):
        csn_document_json_filename = FosAppArgsUtil.get_tenant_agnostic_csn_json_filename(derived_data_product_metadata)
        csn_document_json_location = FosAppArgsUtil.get_csn_document_json_location(csn_document_json_filename)
        tenant_agnostic_hdlfs_path = FosAppArgsUtil.get_tenant_agnostic_hdlfs_path(hdlfs_config)
        tenant_agnostic_csn_dict = self.fos_data_load_util.read_json(tenant_agnostic_hdlfs_path, csn_document_json_location)
        tenant_agnostic_csn = CSNInteropRoot.from_dict(tenant_agnostic_csn_dict)
        json_csn = tenant_agnostic_csn.to_json()
        matched_derived_data_product_entity_name = tenant_agnostic_csn.retrieve_matching_entity_name_from_csn(
            derived_data_product_to_its_coordinates_dict[derived_data_product]["name"]
        )
        df = tenant_agnostic_csn.map_entity_to_dataframe(matched_derived_data_product_entity_name, result_df)
        entity_annotations = tenant_agnostic_csn.definitions[matched_derived_data_product_entity_name].additional_properties
        meta = tenant_agnostic_csn.meta.to_dict()
        dp_delta = DPDataLakeServiceClient(self.spark).delta_table_uri_builder()
        table_path = self.fos_data_load_util.get_delta_path(
            dp_delta,
            self.context.tenant_id,
            "gold",
            derived_data_product_to_its_coordinates_dict[derived_data_product]["version"],
            derived_data_product_to_its_coordinates_dict[derived_data_product]["namespace"],
            matched_derived_data_product_entity_name,
        )
        return {
            "df": df,
            "json_csn": json_csn,
            "table_path": table_path,
            "matched_derived_data_product_entity_name": matched_derived_data_product_entity_name,
            "namespace": derived_data_product_to_its_coordinates_dict[derived_data_product]["namespace"],
            "version": derived_data_product_to_its_coordinates_dict[derived_data_product]["version"]
        }

    def _write_and_register_table(self, csn_info, tenant_id, derived_data_product_to_its_coordinates_dict, derived_data_product):
        df = csn_info["df"]
        table_path = csn_info["table_path"]
        json_csn = csn_info["json_csn"]
        matched_derived_data_product_entity_name = csn_info["matched_derived_data_product_entity_name"]
        namespace = csn_info["namespace"]
        version = csn_info["version"]
        df.write.format("delta").mode("overwrite").save(table_path)
        sql_command_text = f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        self.context.spark.sql(sql_command_text)
        tenant_table_registry = TenantTableRegistry(self.context.dp_spark, tenant_id)
        tenant_table_registry.register_tables(
            derived_data_product_to_its_coordinates_dict[derived_data_product],
            "gold",
            namespace,
            matched_derived_data_product_entity_name,
            version,
            json_csn,
        )
    def _load_delta_table(self, dependent_tables_dict, key, tenant_id):
        return self.get_spark().read.format("delta").load(
            self.retrieve_target_path(
                dependent_tables_dict,
                key,
                self.dp_delta,
                tenant_id,
            )
        )
    def run_transformation(self, dependent_tables_dict, tenant_id, transformer_parameters_dict):
        sales_contract_df = self._load_all_delta_tables(dependent_tables_dict, tenant_id)
        result_df = sales_contract_df.select(
            "SalesContract",
            "SalesContractType",
            "SalesOrganization",
            "DistributionChannel",
            "OrganizationDivision"
        )
        result_df.show(truncate=False)
        result_df.printSchema()
        return result_df

    def register_tables(self, result_df, derived_data_product_metadata, hdlfs_config, dependent_tables_dict, derived_data_product_to_its_coordinates_dict, derived_data_product, tenant_id):
        csn_info = self._prepare_csn_info(
            derived_data_product_metadata,
            hdlfs_config,
            dependent_tables_dict,
            derived_data_product_to_its_coordinates_dict,
            derived_data_product,
            result_df
        )
        self._write_and_register_table(
            csn_info,
            tenant_id,
            derived_data_product_to_its_coordinates_dict,
            derived_data_product
        )
        self.logger.info(
            f"Table registration successful for: {csn_info['table_path']}  under: {derived_data_product_to_its_coordinates_dict[derived_data_product]}"
        )


def main():
    """
    Entrypoint for the transformation job. This function is required for launchers that expect a 'main' method in the module.
    """
    BaseTransformationJob.main(ProcodeDdpTemplate_transformationJob)

if __name__ == "__main__":
    main()