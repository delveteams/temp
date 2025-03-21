from kedro.pipeline import Pipeline, node, pipeline

from .nodes import add_product_name_SKU, barplot_of_available_inventory_per_warehouse, experiment_metrics, \
    merge_tables, metrics, preprocess_bergenInventory_products, preprocess_quota, preprocess_sku, \
    preprocess_tplCenter, quota_barplot, stacked_barplot, total_inventory


# SKU_NJ_barplot, SKU_PA_barplot, SKU_TPLC_barplot,
def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=preprocess_bergenInventory_products,
            inputs=["bergenInventoryNJ"],
            outputs="bergenInventoryNJ_preprocessed",
            name="preprocess_bergenInventoryNJ_node",
        ),
        node(
            func=preprocess_tplCenter,
            inputs=["tplCenter", "SKUs_preprocessed"],
            outputs="tplCenter_preprocessed",
            name="preprocess_tplCenter_node",
        ),
        node(
            func=preprocess_quota,
            inputs=["retailQuota"],
            outputs="retailQuota_preprocessed",
            name="preprocess_quota_node",
            
        ),
        node(
            func=preprocess_sku,
            inputs=["SKUs"],
            outputs="SKUs_preprocessed",
            name="preprocess_SKUs_node",    
        ),
        node(
            func=merge_tables,
            inputs=["bergenInventoryNJ_preprocessed", "tplCenter_preprocessed"],
            outputs="merged_table",
            name="merge_table_node",
        ),
        node(
            func=metrics,
            inputs=["merged_table", "retailQuota_preprocessed", "all_SKU_shopify"],
            outputs="metrics_table",
            name="metrics_node",
        ),
        node(
            func=experiment_metrics, 
            inputs=["final_SKU_table"],
            outputs="experiment_metrics",
            name="experiment_metrics_node",    
        ),
       
        node(
            func=barplot_of_available_inventory_per_warehouse,
            inputs=["merged_table"],
            outputs="barplot_of_available_inventory_per_warehouse",
            name="warehouse_available_barplot_node",
        ),
        
        node(
            func=quota_barplot,
            inputs=["metrics_table", "SKUs_preprocessed"],
            outputs="quota_barplot",
            name="quota_barplot",
        ),
        node(
            func=add_product_name_SKU,
            inputs=["metrics_table", "SKUs_preprocessed", "retailPrice"],
            outputs="final_SKU_table",
            name="add_product_name_SKU_node",
        ),
        node(
            func=stacked_barplot,
            inputs=["merged_table", "SKUs_preprocessed"],
            outputs="stacked_barplot_for_sku",
            name="stacked_barplot_node",
        ),
        node(
            func=total_inventory,
            inputs=["final_SKU_table"],
            outputs="total_inventory",
            name="total_inventory_node",
        )
        #  node(
        #     func=test, 
        #     inputs=["bergenInventoryPAAPI"],
        #     outputs="bergenI",
        #     name="bergenI",
        # ),
        #  node(
        #      lambda x: print(x.xml),
        #     inputs=["bergenInventoryPAAPI"],
        #     outputs=None
        #  ),
        # node(
        #     func=SKU_PA_barplot, 
        #     inputs=["final_SKU_table"],
        #     outputs="SKU_PA_barplot",
        #     name="SKU_PA_barplot_node",
        # ),
        # node(
        #     func=SKU_NJ_barplot,
        #     inputs=["metrics_table", "SKUs_preprocessed"],
        #     outputs="SKU_NJ_barplot",
        #     name="SKU_NJ_barplot_node",
        # ),
        # node(
        #     func=SKU_TPLC_barplot,
        #     inputs=["metrics_table", "SKUs_preprocessed"],
        #     outputs="SKU_TPLC_barplot",
        #     name="SKU_TPLC_barplot_node",
        # ),
    ])
