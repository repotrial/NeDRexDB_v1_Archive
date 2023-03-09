import biocypher
import sys

sys.path.append("")
from adapters.target_disease_evidence_adapter import (
    NeDRexAdapter,
    NeDRexDataset,
    DrugNodeField, DiseaseNodeField, GeneNodeField, ProteinNodeField, SignatureNodeField, PathwayNodeField,
    DrugDiseaseIndicationEdgeField
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

node_fields = [
    # mandatory fields
    DrugNodeField._PRIMARY_ID,
    DiseaseNodeField._PRIMARY_ID,
    GeneNodeField._PRIMARY_ID,
    ProteinNodeField._PRIMARY_ID,
    SignatureNodeField._PRIMARY_ID,
    PathwayNodeField._PRIMARY_ID,

    # optional_fileds
    DrugNodeField.DRUG_NAME,
    DrugNodeField.ALL_DATASETS,
    DrugNodeField.CAS_NUMBER,
    DrugNodeField.DESCRIPTION,
    DrugNodeField.DOMAIN_IDS,
    DrugNodeField.DRUG_CATEGORIES,
    DrugNodeField.DRUG_GROUPS,
    DrugNodeField.INDICATION,
    DrugNodeField.IUPAC_NAME,
    DrugNodeField.PRIMARY_DATASET,
    DrugNodeField.SEQUENCES,
    DrugNodeField.SYNONYMS,
    DrugNodeField.INCI,
    DrugNodeField.MOLECULAR_FORMULA,
    DrugNodeField.SMILES,

    DiseaseNodeField.NAME,
    DiseaseNodeField.DomainIDs,
    DiseaseNodeField.SYNONYMS,
    DiseaseNodeField.ICD10,
    DiseaseNodeField.DESCRIPTION,

    GeneNodeField.NAME,
    GeneNodeField.DOMAIN_IDS,
    GeneNodeField.SYNONYMS,
    GeneNodeField.APPROVED_SYMBOL,
    GeneNodeField.SYMBOLS,
    GeneNodeField.DESCRIPTION,
    GeneNodeField.CHROMOSOME,
    GeneNodeField.MAP_LOCATION,
    GeneNodeField.GENE_TYPE,

    ProteinNodeField.NAME,
    ProteinNodeField.SEQUENCE,
    ProteinNodeField.DISPLAY_NAME,
    ProteinNodeField.SYNONYMS,
    ProteinNodeField.COMMENTS,
    ProteinNodeField.GENE_NAME,
    ProteinNodeField.TAXID,

    PathwayNodeField.NAME,
    PathwayNodeField.DOMAIN_IDS,
    PathwayNodeField.SPECIES,

    SignatureNodeField.DESCRIPTION
]
edge_fields = [
    # mandatory fields
    DrugDiseaseIndicationEdgeField._PRIMARY_TARGET_ID,
    DrugDiseaseIndicationEdgeField._PRIMARY_SOURCE_ID

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
        node_fields=node_fields,
        edge_fields=edge_fields

    )

    nedrex_adapter.load_data(
        stats=False,
        show_nodes=False,
        show_edges=False,
    )

    # Write nodes
    driver.write_nodes(nedrex_adapter.get_nodes())

    # Write OTAR edges in batches to avoid memory issues

    driver.write_edges(nedrex_adapter.get_edges())

    # Post import functions
    driver.write_import_call()
    driver.log_duplicates()
    driver.log_missing_bl_types()


if __name__ == "__main__":
    main()
