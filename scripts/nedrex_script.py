import biocypher
import sys

sys.path.append("")
from adapters.nedrex_adapter import (
    NeDRexAdapter,
    NeDRexDataset,
    DrugNodeField, DiseaseNodeField, GeneNodeField, ProteinNodeField, SignatureNodeField, PathwayNodeField,
    DrugDiseaseIndicationEdgeField, DiseaseDiseaseEdgeField, DrugTargetEdgeField, DrugDiseaseContraindicationEdgeField,
    GeneDiseaseAssociationEdgeField
, DrugSimilarityEdgeField, ProteinProteinSimilarityEdgeField, ProteinHasSignatureEdgeField, ProteinEncodedByEdgeField,
    ProteinIsInPathwayEdgeField, ProteinIsIsoformOfProteinEdgeField, ProteinProteinInteractionEdgeField
)

"""
Configuration: select datasets and fields to be imported.

`node_field`: list of fields to be imported for each of the types of nodes that
the adapter creates. See nedrex_adapter.py for available fields
or use Enum auto-complete of `...NodeField`. Note
that some fields are mandatory for the functioning of the adapter (primary
identifiers) and some are optional (e.g. gene symbol).

`edge_fields`: list of fields to be imported for each of the relationships that
the adapter creates. See nedrex_adapter.py for available fields
or use Enum auto-complete of `...EdgeField`. Note that some fields are
mandatory for the functioning of the adapter (primary identifiers) and some are
optional (e.g.  score).
"""

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
    DrugSimilarityEdgeField.MORGAN_R1,
    DrugSimilarityEdgeField.MORGAN_R2,
    DrugSimilarityEdgeField.MORGAN_R3,
    DrugSimilarityEdgeField.MORGAN_R4,
    DrugSimilarityEdgeField.MACCS,

    ProteinProteinSimilarityEdgeField.BLAST12_QUERY,
    ProteinProteinSimilarityEdgeField.BLAST12_HIT,
    ProteinProteinSimilarityEdgeField.BLAST12_BITSCORE,
    ProteinProteinSimilarityEdgeField.BLAST12_QUERY_E_VALUE,
    ProteinProteinSimilarityEdgeField.BLAST12_QUERY_START,
    ProteinProteinSimilarityEdgeField.BLAST12_QUERY_END,
    ProteinProteinSimilarityEdgeField.BLAST12_HIT_START,
    ProteinProteinSimilarityEdgeField.BLAST12_HIT_END,
    ProteinProteinSimilarityEdgeField.BLAST12_IDENTITY,
    ProteinProteinSimilarityEdgeField.BLAST12_MISMATCHES,
    ProteinProteinSimilarityEdgeField.BLAST12_GAPS,
    ProteinProteinSimilarityEdgeField.BLAST12_QUERY_COVER,
    ProteinProteinSimilarityEdgeField.BLAST12_HIT_COVER,
    ProteinProteinSimilarityEdgeField.BLAST12_TYPE,
    ProteinProteinSimilarityEdgeField.BLAST21_QUERY,
    ProteinProteinSimilarityEdgeField.BLAST21_HIT,
    ProteinProteinSimilarityEdgeField.BLAST21_BITSCORE,
    ProteinProteinSimilarityEdgeField.BLAST21_E_VALUE,
    ProteinProteinSimilarityEdgeField.BLAST21_QUERY_START,
    ProteinProteinSimilarityEdgeField.BLAST21_QUERY_END,
    ProteinProteinSimilarityEdgeField.BLAST21_HIT_START,
    ProteinProteinSimilarityEdgeField.BLAST21_HIT_END,
    ProteinProteinSimilarityEdgeField.BLAST21_IDENTITY,
    ProteinProteinSimilarityEdgeField.BLAST21_MISMATCHES,
    ProteinProteinSimilarityEdgeField.BLAST21_GAPS,
    ProteinProteinSimilarityEdgeField.BLAST21_QUERY_COVER,
    ProteinProteinSimilarityEdgeField.BLAST21_HIT_COVER,
    ProteinProteinSimilarityEdgeField.BLAST21_TYPE,

    ProteinProteinInteractionEdgeField.DATABASES,
    ProteinProteinInteractionEdgeField.METHODS,
    ProteinProteinInteractionEdgeField.EVIDENCE_TYPES,

    GeneDiseaseAssociationEdgeField.ASSERTED_BY,
    GeneDiseaseAssociationEdgeField.SCORE
]


def main():
    """
    Main function running the import using BioCypher and the adapter.
    """

    # Start BioCypher
    driver = biocypher.Driver(
        db_name="nedrex",
        offline=True,
        wipe=True,
        output_directory="biocypher-out/nedrex",
        import_call_file_prefix="import/nedrex/",
        user_schema_config_path="config/nedrex_schema_config.yaml",
        skip_bad_relationships=True,  # allows import of incomplete test data
    )

    # Check the schema
    driver.show_ontology_structure()

    # Load data

    # NeDRex Adapter
    nedrex_adapter = NeDRexAdapter(
        node_fields=node_fields,
        edge_fields=edge_fields
    )

    nedrex_adapter.load_data(
        stats=False,
        show_nodes=False
    )

    # Write NeDRex nodes nodes
    driver.write_nodes(nedrex_adapter.get_nodes())

    # Write NeDRex edges in batches to avoid memory issues

    driver.write_edges(nedrex_adapter.get_edges())

    # Post import functions
    driver.write_import_call()
    driver.log_duplicates()
    driver.log_missing_bl_types()


if __name__ == "__main__":
    main()
