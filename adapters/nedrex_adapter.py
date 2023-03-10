from typing import Optional
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, functions as F
from enum import Enum
from bioregistry import normalize_curie
from biocypher._logger import logger
from tqdm import tqdm
import functools
from functools import reduce


class NeDRexDataset(Enum):
    """
    Enum of all the datasets used in the target-disease evidence pipeline.
    Values are the spellings used in the Open Targets parquet files.
    """

    DRUGBANK_DRUG = "drugbank_drugs"


_licences = {
    "drugbank_node": "CC BY-NC 4.0",
    "mondo_node": "CC BY 4.0",
    "drugcentral_edge": "CC BY-SA 4.0",
    "ctd_edge": "NA",
    "uniprot_node": "CC BY 4.0",
    "ncbi_node": "NA",
    "reactome_node": "CC0 1.0 Universal",
    "mondo_edge": "CC BY 4.0",
    "omim_edge": "https://wolken.zbh.uni-hamburg.de/index.php/s/okAEGrEH3WP5F6w/download/NeDRexDB_v1_license.txt",
    "disgenet_edge": "CC BY-NC-SA 4.0",
    "uniprot_edge": "CC BY 4.0",
    "reactome_edge": "CC0 1.0 Universal",
    "biogrid_edge":"MIT",
    "iid_edge":"NA",
    "intact_edge":"NA"
}


class DrugNodeField(Enum):
    """
    Enum of all the fields in the target dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    DRUGBANK_ID = "primaryDomainId"
    _PRIMARY_ID = DRUGBANK_ID
    SOURCE = "drugbank"
    VERSION = "v5.1"

    # optional fields
    DRUG_NAME = "displayName"
    ALL_DATASETS = "allDatasets"
    CAS_NUMBER = "casNumber"
    DESCRIPTION = "description"
    DOMAIN_IDS = "domainIds"
    DRUG_CATEGORIES = "drugCategories"
    DRUG_GROUPS = "drugGroups"
    INDICATION = "indication"
    IUPAC_NAME = "iupacName"
    PRIMARY_DATASET = "primaryDataset"
    SEQUENCES = "sequences"
    SYNONYMS = "synonyms"
    INCI = "inchi"
    MOLECULAR_FORMULA = "molecularFormula"
    SMILES = "smiles"


class DiseaseNodeField(Enum):
    """
    Enum of all the fields in the disease dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    MONDO_ID = "primaryDomainId"
    _PRIMARY_ID = MONDO_ID
    SOURCE = "mondo"
    VERSION = "release 2022-03-01"

    #     # optional fields
    NAME = "displayName"
    DomainIDs = "domainIds"
    SYNONYMS = "synonyms"
    ICD10 = "icd10"
    DESCRIPTION = "description"


class GeneNodeField(Enum):
    """
    Enum of all the fields in the disease dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    ENTREZ_ID = "primaryDomainId"
    _PRIMARY_ID = ENTREZ_ID
    SOURCE = "ncbi"
    VERSION = "NA"
    #
    #     # optional fields
    NAME = "displayName"
    DOMAIN_IDS = "domainIds"
    SYNONYMS = "synonyms"
    APPROVED_SYMBOL = "approvedSymbol"
    SYMBOLS = "symbols"
    DESCRIPTION = "description"
    CHROMOSOME = "chromosome"
    MAP_LOCATION = "mapLocation"
    GENE_TYPE = "geneType"


class ProteinNodeField(Enum):
    """
    Enum of all the fields in the disease dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    UNIPROT_ID = "primaryDomainId"
    _PRIMARY_ID = UNIPROT_ID
    SOURCE = "uniprot"
    VERSION = "release 2022_01"
    #
    #     # optional fields
    NAME = "displayName"
    SEQUENCE = "sequence"
    DISPLAY_NAME = "displayName"
    SYNONYMS = "synonyms"
    COMMENTS = "comments"
    GENE_NAME = "geneName"
    TAXID = "taxid"


class PathwayNodeField(Enum):
    """
    Enum of all the fields in the disease dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    KEGG_ID = "primaryDomainId"
    _PRIMARY_ID = KEGG_ID
    SOURCE = "reactome"
    VERSION = "v79"
    #
    #     # optional fields
    NAME = "displayName"
    DOMAIN_IDS = "domainIds"
    SPECIES = "species"


class SignatureNodeField(Enum):
    """
    Enum of all the fields in the disease dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    PROSITE_ID = "primaryDomainId"
    _PRIMARY_ID = PROSITE_ID
    SOURCE = "uniprot"
    VERSION = "release 2022_01"
    #
    #     # optional fields
    DESCRIPTION = "description"


#
class DrugDiseaseIndicationEdgeField(Enum):
    # mandatory fields
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ['drugcentral', 'ctd']
    VERSIONS = ['2021-05-10', 'release March 2022']


class DiseaseDiseaseEdgeField(Enum):
    # mandatory fields
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ['mondo']
    VERSIONS = ['release 2022-03-01']


class DrugDiseaseContraindicationEdgeField(Enum):
    # mandatory fields
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ['drugcentral', 'ctd']
    VERSIONS = ['2021-05-10', 'release March 2022']


class DrugTargetEdgeField(Enum):
    # mandatory fields
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ['drugcentral', 'ctd']
    VERSIONS = ['2021-05-10', 'release March 2022']


class GeneDiseaseAssociationEdgeField(Enum):
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ['omim', 'disgenet']
    VERSIONS = ['release March 21 2022', 'v7.0']

    ASSERTED_BY = "assertedBy"
    SCORE = "score"


class ProteinIsIsoformOfProteinEdgeField(Enum):
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ['uniprot']
    VERSIONS = ["release 2022_01"]


class DrugSimilarityEdgeField(Enum):
    TARGET_DISEASE = "memberTwo"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "memberOne"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ["NA"]
    VERSIONS = ["NA"]

    MORGAN_R1 = "morganR1"
    MORGAN_R2 = "morganR2"
    MORGAN_R3 = "morganR3"
    MORGAN_R4 = "morganR4"
    MACCS = "maccs"


class ProteinEncodedByEdgeField(Enum):
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ["uniprot"]
    VERSIONS = ["release 2022_01"]


class ProteinHasSignatureEdgeField(Enum):
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ["uniprot"]
    VERSIONS = ["release 2022_01"]


class ProteinIsInPathwayEdgeField(Enum):
    TARGET_DISEASE = "targetDomainId"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "sourceDomainId"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ["reactome"]
    VERSIONS = ["v79"]

class ProteinProteinInteractionEdgeField(Enum):
    TARGET_DISEASE = "memberTwo"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "memberOne"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ["biogrid","iid","intact"]
    VERSIONS = ["v4.4.207", "v2021-05", "v1.0.3"]

    METHODS = 'methods'
    DATABASES = 'databases'
    EVIDENCE_TYPES = 'evidenceTypes'

class ProteinProteinSimilarityEdgeField(Enum):
    TARGET_DISEASE = "memberTwo"
    _PRIMARY_TARGET_ID = TARGET_DISEASE

    SOURCE_DRUG = "memberOne"
    _PRIMARY_SOURCE_ID = SOURCE_DRUG

    SOURCES = ["NA"]
    VERSIONS = ["NA"]

    BLAST12_QUERY = "blast12_query"
    BLAST12_HIT = "blast12_hit"
    BLAST12_BITSCORE = "blast12_bitscore"
    BLAST12_QUERY_E_VALUE = "blast12_evalue"
    BLAST12_QUERY_START = "blast12_queryStart"
    BLAST12_QUERY_END = "blast12_queryEnd"
    BLAST12_HIT_START = "blast12_hitStart"
    BLAST12_HIT_END = "blast12_hitEnd"
    BLAST12_IDENTITY = "blast12_identity"
    BLAST12_MISMATCHES = "blast12_mismatches"
    BLAST12_GAPS = "blast12_gaps"
    BLAST12_QUERY_COVER = "blast12_queryCover"
    BLAST12_HIT_COVER = "blast12_hitCover"
    BLAST12_TYPE= "blast12_type"
    BLAST21_QUERY = "blast21_query"
    BLAST21_HIT = "blast21_hit"
    BLAST21_BITSCORE = "blast21_bitscore"
    BLAST21_E_VALUE = "blast21_evalue"
    BLAST21_QUERY_START = "blast21_queryStart"
    BLAST21_QUERY_END = "blast21_queryEnd"
    BLAST21_HIT_START = "blast21_hitStart"
    BLAST21_HIT_END = "blast21_hitEnd"
    BLAST21_IDENTITY = "blast21_identity"
    BLAST21_MISMATCHES = "blast21_mismatches"
    BLAST21_GAPS = "blast21_gaps"
    BLAST21_QUERY_COVER = "blast21_queryCover"
    BLAST21_HIT_COVER = "blast21_hitCover"
    BLAST21_TYPE = "blast21_type"


class NeDRexAdapter:
    def __init__(
            self,
            datasets: list[Enum] = None,
            node_fields: list[Enum] = None,
            edge_fields: list = None,
    ):
        self.node_fields = node_fields
        self.edge_fields = edge_fields

        if not self.node_fields:
            raise ValueError("node_fields must be provided")

        if not self.edge_fields:
            raise ValueError("edge_fields must be provided")

        # if not DrugNodeField._PRIMARY_ID in self.node_fields:
        #     raise ValueError(
        #         "DrugNodeField._PRIMARY_ID must be provided"
        #     )

        # if not DiseaseNodeField.DISEASE_ACCESSION in self.node_fields:
        #     raise ValueError(
        #         "DiseaseNodeField.DISEASE_ACCESSION must be provided"
        #     )

        logger.info("Creating Spark session.")

        # Set up Spark context
        conf = (
            SparkConf()
            .setAppName("nedrex_biocypher")
            .setMaster("local")
            .set("spark.driver.memory", "4g")
            .set("spark.executor.memory", "4g")
        )
        self.sc = SparkContext(conf=conf)

        # Create SparkSession
        self.spark = (
            SparkSession.builder.master("local")
            .appName("nedrex_biocypher")
            .getOrCreate()
        )

    def download_data(self, version: str, force: bool = False):
        """
        Download datasets from Open Targets website. Manage downloading and
        caching of files. TODO

        Args:

            version: Version of the Open Targets data to download.

            force: Whether to force download of files even if they already
            exist.
        """
        pass

    def process_headers(self, data):
        headers = data.schema.names
        new_headers = [header.split(":")[0] if ":" in header else header for header in headers]
        new_headers = ["_".join(header.split(".")) if '.' in header else header for header in new_headers]
        return reduce(lambda data, idx: data.withColumnRenamed(headers[idx], new_headers[idx]), range(len(headers)),
                      data)

    def load_data(
            self,
            stats: bool = False,
            show_nodes: bool = False
    ):
        """
        Load data from disk into Spark DataFrames.

        Args:

            stats: Whether to print out schema and counts of nodes and edges.

            show_nodes: Whether to print out the first row of each node
            dataframe.

            show_edges: Whether to print out the first row of each edge
            dataframe.
        """

        logger.info("Loading Open Targets data from disk.")

        # Read in evidence data and target / disease annotations
        prefix = "data/nedrex_files/"

        def get_path(file):
            return prefix + file

        self.drugs = self.process_headers(self.spark.read.option("header", True).csv(get_path('drug.csv')))
        self.disorders = self.process_headers(self.spark.read.option("header", True).csv(get_path('disorder.csv')))
        self.genes = self.process_headers(self.spark.read.option("header", True).csv(get_path('gene.csv')))
        self.proteins = self.process_headers(self.spark.read.option("header", True).csv(get_path('protein.csv')))
        self.pathways = self.process_headers(self.spark.read.option("header", True).csv(get_path('pathway.csv')))
        self.signatures = self.process_headers(self.spark.read.option("header", True).csv(get_path('signature.csv')))

        self.disorder_is_subtype_of_disorder = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('disorder_is_subtype_of_disorder.csv')))
        self.drug_has_indication = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('drug_has_indication.csv')))
        self.drug_has_contraindication = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('drug_has_contraindication.csv')))
        self.drug_has_target = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('drug_has_target.csv')))

        self.gene_associated_with_disorder = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('gene_associated_with_disorder.csv')))
        self.is_isoform_of = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('is_isoform_of.csv')))
        self.molecule_similarity_molecule = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('molecule_similarity_molecule.csv')))
        self.protein_encoded_by = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('protein_encoded_by.csv')))

        self.protein_has_signature = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('protein_has_signature.csv')))
        self.protein_in_pathway = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('protein_in_pathway.csv')))
        self.protein_interacts_with_protein = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('protein_interacts_with_protein.csv')))
        self.protein_similarity_protein = self.process_headers(
            self.spark.read.option("header", True).csv(get_path('protein_similarity_protein.csv')))


        if stats:
            # print schema
            print(self.drugs.printSchema())
            print(self.disorders.printSchema())
            print(self.genes.printSchema())
            print(self.proteins.printSchema())
            print(self.signatures.printSchema())
            print(self.pathways.printSchema())


            # print number of rows
            print(
                f"Length of evidence data: {self.drugs.count()} entries"
            )
            print(
                f"Length of evidence data: {self.disorders.count()} entries"
            )
            print(
                f"Length of evidence data: {self.genes.count()} entries"
            )
            print(
                f"Length of evidence data: {self.proteins.count()} entries"
            )
            print(
                f"Length of evidence data: {self.signatures.count()} entries"
            )
            print(
                f"Length of evidence data: {self.pathways.count()} entries"
            )


        if show_nodes:
            self.drugs.show(1, 50, True)
            self.disorders.show(1, 50, True)
            self.genes.show(1, 50, True)
            self.proteins.show(1, 50, True)
            self.pathways.show(1, 50, True)
            self.signatures.show(1, 50, True)


    def _yield_nodes(
            self,
            df,
            node_field_type: Enum,
            ontology_class: Optional[str] = None,
    ):
        """
        Yield the node type from the dataframe.

        Args:

            df: Spark DataFrame containing the node data.

            node_field_type: Enum containing the node fields.

            ontology_class: Ontological class of the node (corresponding to the
            `label_in_input` field in the schema configuration).
        """

        # Select columns of interest

        logger.info(f"Generating nodes of {node_field_type}.")

        for row in tqdm(df.collect()):

            # normalize id
            _id, _type = _process_id_and_type(
                row[node_field_type._PRIMARY_ID.value], ontology_class
            )

            if not _id:
                continue

            _props = {}
            _props["version"] = node_field_type.VERSION.value
            _props["source"] = node_field_type.SOURCE.value
            _props["licence"] = _find_licence(node_field_type.SOURCE.value + "_node")

            for field in self.node_fields:

                if not isinstance(field, node_field_type):
                    continue

                if row[field.value]:
                    _props[field.value] = row[field.value]

            yield (_id, _type, _props)

    def get_nodes(self):
        """
        Yield nodes from the target and disease dataframes.
        """

        # Drugs
        yield from self._yield_nodes(self.drugs, DrugNodeField)

        # Diseases
        yield from self._yield_nodes(self.disorders, DiseaseNodeField)

        # Genes
        yield from self._yield_nodes(self.genes, GeneNodeField)

        # # Proteins
        yield from self._yield_nodes(self.proteins, ProteinNodeField)

        # # Pathways
        yield from self._yield_nodes(self.pathways, PathwayNodeField)

        # # Signatures
        yield from self._yield_nodes(self.signatures, SignatureNodeField, 'prosite')

    def get_edge_batches(self, df):
        """
        Create a column with partition number in the evidence dataframe and
        return a list of batch numbers.
        """

        logger.info("Generating batches.")

        # add partition number to self.evidence_df as column
        df = df.withColumn(
            "partition_num", F.spark_partition_id()
        )
        df.persist()

        self.batches = [
            int(row.partition_num)
            for row in df.select("partition_num")
            .distinct()
            .collect()
        ]

        logger.info(f"Generated {len(self.batches)} batches.")

        return df, self.batches

    def get_edges(self):
        """
        Yield edges from the evidence dataframe per batch.
        """

        # Check if self.evidence_df has column partition_num

        logger.info("Generating edges.")

        dfs = [(self.drug_has_indication, DrugDiseaseIndicationEdgeField),
               (self.disorder_is_subtype_of_disorder, DiseaseDiseaseEdgeField),
               (self.drug_has_contraindication, DrugDiseaseContraindicationEdgeField),
               (self.drug_has_target, DrugTargetEdgeField),
               (self.gene_associated_with_disorder, GeneDiseaseAssociationEdgeField),
               (self.is_isoform_of, ProteinIsIsoformOfProteinEdgeField),
               (self.molecule_similarity_molecule, DrugSimilarityEdgeField),
               (self.protein_encoded_by, ProteinEncodedByEdgeField),
               (self.protein_has_signature, ProteinHasSignatureEdgeField),
               (self.protein_in_pathway, ProteinIsInPathwayEdgeField),
               (self.protein_interacts_with_protein, ProteinProteinInteractionEdgeField),
               (self.protein_similarity_protein, ProteinProteinSimilarityEdgeField)
               ]

        id_types = {ProteinHasSignatureEdgeField : (None,'prosite')}

        for df, edge_field_type in dfs:
            df, batches = self.get_edge_batches(df)
            for batch in batches:
                yield from self._process_edges(
                    batch=df.where(
                        df.partition_num == batch
                    ), edge_field_type=edge_field_type, source_type=id_types.get(edge_field_type, (None,None))[0], target_type=id_types.get(edge_field_type,(None,None))[1]
                )

    def _process_edges(self, batch, edge_field_type: Enum, source_type=None, target_type=None):
        """
        Process one batch of edges.

        Args:

            batch: Spark DataFrame containing the edges of one batch.
        """

        logger.info(f"Batch size: {batch.count()} edges for {edge_field_type}.")

        # yield edges per row of edge_df, skipping null values
        for row in tqdm(batch.collect()):
            # collect properties from fields, skipping null values
            properties = {}

            properties["version"] = edge_field_type.VERSIONS.value
            properties["source"] = edge_field_type.SOURCES.value
            properties['licence'] = [_find_licence(source + "_edge") for source in properties["source"]]

            for field in self.edge_fields:

                if not isinstance(field, edge_field_type):
                    continue

                if row[field.value]:
                    properties[field.value] = row[field.value]

            source_id, _ = _process_id_and_type(row[edge_field_type._PRIMARY_SOURCE_ID.value], target_type)
            target_id, _ = _process_id_and_type(row[edge_field_type._PRIMARY_TARGET_ID.value], source_type)

            yield (
                0,
                source_id,
                target_id,
                row.type,
                properties,
            )


@functools.lru_cache()
def _process_id_and_type(inputId: str, _type: Optional[str] = None):
    """
    Process diseaseId and diseaseType fields from evidence data. Process
    gene (ENSG) ids.

    Args:

        inputId: id of the node.

        _type: type of the node.
    """

    if not inputId:
        return (None, None)
    if _type:
        id = inputId.split(".")[1]
        _id = normalize_curie(f"{_type}:{id}")

        return (_id, _type)

    # detect delimiter (either _ or :)
    if "." in inputId:
        _type = inputId.split(".")[0].lower()

        _id = normalize_curie(inputId, sep=".")

        return (_id, _type)
    return (inputId, None)


def _find_licence(source: str) -> str:
    """
    Find and return the licence for a source.

    Args:

        source: source of the evidence. Spelling as in the Open Targets
        evidence data.
    """

    return _licences.get(source, "Unknown")
