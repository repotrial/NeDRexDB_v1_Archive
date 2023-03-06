import biocypher
import sys

sys.path.append("")
from adapters.target_disease_evidence_adapter import (
    NeDRexAdapter,
    NeDRexDataset,
    DrugNodeField,
    # DiseaseNodeField,
    # TargetDiseaseEdgeField,
    # GeneOntologyNodeField,
    # MousePhenotypeNodeField,
    # MouseTargetNodeField,
)

"""
Configuration: select datasets and fields to be imported.

`datasets`: list of datasets to be imported. See 
target_disease_evidence_adapter.py for available datasets or use
`TargetDiseaseDataset` Enum auto-complete.

`node_field`: list of fields to be imported for each of the types of nodes that
the adapter creates. See target_disease_evidence_adapter.py for available fields
or use Enum auto-complete of `TargetNodeField`, `DiseaseNodeField`,
`GeneOntologyNodeField`, `MousePhenotypeNodeField`, `MouseTargetNodeField`. Note
that some fields are mandatory for the functioning of the adapter (primary
identifiers) and some are optional (e.g. gene symbol).

`edge_fields`: list of fields to be imported for each of the relationships that
the adapter creates. See target_disease_evidence_adapter.py for available fields
or use Enum auto-complete of `TargetDiseaseEdgeField`. Note that some fields are
mandatory for the functioning of the adapter (primary identifiers) and some are
optional (e.g.  score).
"""

target_disease_datasets = [
    NeDRexDataset.DRUGBANK_DRUG
]

drug_node_fields = [
    # mandatory fields
    DrugNodeField.DRUG_ID,
    DrugNodeField.DRUG_NAME
]

target_disease_edge_fields = [
    # mandatory fields
    # TargetDiseaseEdgeField.INTERACTION_ACCESSION,
    # TargetDiseaseEdgeField.TARGET_GENE_ENSG,
    # TargetDiseaseEdgeField.DISEASE_ACCESSION,
    # TargetDiseaseEdgeField.TYPE,
    # TargetDiseaseEdgeField.SOURCE,
    # # optional fields
    # TargetDiseaseEdgeField.SCORE,
    # TargetDiseaseEdgeField.LITERATURE,
]


def main():
    """
    Main function running the import using BioCypher and the adapter.
    """

    # Start BioCypher
    driver = biocypher.Driver(
        db_name="nedrex",
        offline=True,
        wipe= True,
        output_directory="biocypher-out/nedrex",
        user_schema_config_path="config/nedrex_schema_config.yaml",
        skip_bad_relationships=True,  # allows import of incomplete test data
    )

    # Check the schema
    # driver.show_ontology_structure()

    # Load data

    # Open Targets
    nedrex_adapter = NeDRexAdapter(
        datasets=target_disease_datasets,
        node_fields=drug_node_fields,
        edge_fields=target_disease_edge_fields,
        test_mode=True,

    )

    nedrex_adapter.load_data(
        stats=False,
        show_nodes=False,
        show_edges=False,
    )

    # Write nodes
    driver.write_nodes(nedrex_adapter.get_nodes())

    # Write OTAR edges in batches to avoid memory issues
    # batches = nedrex_adapter.get_edge_batches()
    # for batch in batches:
    #     driver.write_edges(nedrex_adapter.get_edges(batch_number=batch))

    # Post import functions
    driver.write_import_call()
    driver.log_duplicates()
    driver.log_missing_bl_types()


if __name__ == "__main__":
    main()
