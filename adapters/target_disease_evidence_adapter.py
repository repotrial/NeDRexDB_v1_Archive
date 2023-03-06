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
    "drugbank_drugs": "CC BY-NC 4.0",
}


class DrugNodeField(Enum):
    """
    Enum of all the fields in the target dataset. Values are the spellings used
    in the Open Targets parquet files.
    """

    # mandatory fields
    DRUG_ID = "primaryDomainId"
    _PRIMARY_ID = DRUG_ID

    # optional fields
    DRUG_NAME = "displayName"


# class DiseaseNodeField(Enum):
#     """
#     Enum of all the fields in the disease dataset. Values are the spellings used
#     in the Open Targets parquet files.
#     """
#
#     # mandatory fields
#     DISEASE_ACCESSION = "id"
#     _PRIMARY_ID = DISEASE_ACCESSION
#
#     # optional fields
#     DISEASE_CODE = "code"
#     DISEASE_DATABASE_XREFS = "dbXRefs"
#     DISEASE_DESCRIPTION = "description"
#     DISEASE_NAME = "name"
#     DISEASE_DIRECT_LOCATION_IDS = "directLocationIds"
#     DISEASE_OBSOLETE_TERMS = "obsoleteTerms"
#     DISEASE_PARENTS = "parents"
#     DISEASE_SKO = "sko"
#     DISEASE_SYNONYMS = "synonyms"
#     DISEASE_ANCESTORS = "ancestors"
#     DISEASE_DESCENDANTS = "descendants"
#     DISEASE_CHILDREN = "children"
#     DISEASE_THERAPEUTIC_AREAS = "therapeuticAreas"
#     DISEASE_INDIRECT_LOCATION_IDS = "indirectLocationIds"
#     DISEASE_ONTOLOGY = "ontology"


# class GeneOntologyNodeField(Enum):
#     """
#     Enum of all the fields in the gene ontology dataset. Values are the
#     spellings used in the Open Targets parquet files.
#     """
#
#     # mandatory fields
#     GENE_ONTOLOGY_ACCESSION = "id"
#     _PRIMARY_ID = GENE_ONTOLOGY_ACCESSION
#
#     # optional fields
#     GENE_ONTOLOGY_NAME = "name"
#
#
# class MousePhenotypeNodeField(Enum):
#     """
#     Enum of all the fields in the mouse phenotype dataset. Values are the
#     spellings used in the Open Targets parquet files.
#     """
#
#     # mandatory fields
#     MOUSE_PHENOTYPE_ACCESSION = "modelPhenotypeId"
#     _PRIMARY_ID = MOUSE_PHENOTYPE_ACCESSION
#
#     # optional fields
#     MOUSE_PHENOTYPE_LABEL = "modelPhenotypeLabel"
#
#
# class MouseTargetNodeField(Enum):
#     """
#     Enum of all the fields in the mouse phenotype dataset related to murine
#     targets of each biological model. Values are the spellings used in the Open
#     Targets parquet files.
#     """
#
#     # mandatory fields
#     MOUSE_TARGET_ENSG = "targetInModelEnsemblId"
#     _PRIMARY_ID = MOUSE_TARGET_ENSG
#
#     # alternative ids
#     MOUSE_TARGET_SYMBOL = "targetInModel"
#     MOUSE_TARGET_MGI = "targetInModelMgiId"
#
#     # human target ensembl id
#     HUMAN_TARGET_ENGS = "targetFromSourceId"
#
#
# class MouseModelNodeField(Enum):
#     """
#     Enum of all the fields in the mouse phenotype dataset related to the mouse
#     model. Values are the spellings used in the Open Targets parquet files.
#     """
#
#     # mandatory fields
#     MOUSE_PHENOTYPE_MODELS = "biologicalModels"
#     _PRIMARY_ID = MOUSE_PHENOTYPE_MODELS
#
#     MOUSE_PHENOTYPE_CLASSES = "modelPhenotypeClasses"
#
#
# class TargetDiseaseEdgeField(Enum):
#     """
#     Enum of all the fields in the target-disease dataset. Used to generate the
#     bulk of relationships in the graph. Values are the spellings used in the
#     Open Targets parquet files.
#     """
#
#     # mandatory fields
#     INTERACTION_ACCESSION = "id"
#
#     TARGET_GENE_ENSG = "targetId"
#     _PRIMARY_SOURCE_ID = TARGET_GENE_ENSG
#
#     DISEASE_ACCESSION = "diseaseId"
#     _PRIMARY_TARGET_ID = DISEASE_ACCESSION
#
#     TYPE = "datatypeId"
#     SOURCE = "datasourceId"
#     LITERATURE = "literature"
#     SCORE = "score"


class NeDRexAdapter:
    def __init__(
        self,
        datasets: list[Enum] = None,
        node_fields: list[Enum] = None,
        edge_fields: list = None,
        test_mode: bool = False,
    ):
        self.datasets = datasets
        self.node_fields = node_fields
        self.edge_fields = edge_fields
        self.test_mode = test_mode

        if not self.datasets:
            raise ValueError("datasets must be provided")

        if not self.node_fields:
            raise ValueError("node_fields must be provided")

        # if not self.edge_fields:
        #     raise ValueError("edge_fields must be provided")

        if not DrugNodeField.DRUG_ID in self.node_fields:
            raise ValueError(
                "TargetNodeField.DRUG_ID must be provided"
            )

        # if not DiseaseNodeField.DISEASE_ACCESSION in self.node_fields:
        #     raise ValueError(
        #         "DiseaseNodeField.DISEASE_ACCESSION must be provided"
        #     )

        if self.test_mode:
            logger.warning(
                "Open Targets adapter: Test mode is enabled. "
                "Only processing 100 rows."
            )

        logger.info("Creating Spark session.")

        # Set up Spark context
        conf = (
            SparkConf()
            .setAppName("otar_biocypher")
            .setMaster("local")
            .set("spark.driver.memory", "4g")
            .set("spark.executor.memory", "4g")
        )
        self.sc = SparkContext(conf=conf)

        # Create SparkSession
        self.spark = (
            SparkSession.builder.master("local")
            .appName("otar_biocypher")
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
        new_headers = [header.split(":")[0] if ":" in header else header for header in  headers]
        return reduce(lambda data, idx: data.withColumnRenamed(headers[idx], new_headers[idx]), range(len(headers)), data)

    def load_data(
        self,
        stats: bool = False,
        show_nodes: bool = False,
        show_edges: bool = False,
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
        drug_path = "data/nedrex_files/drug.csv"

        self.drugs = self.process_headers(self.spark.read.option("header", True).csv(drug_path))


        # target_path = "data/ot_files/targets"
        # self.target_df = self.spark.read.parquet(target_path)
        #
        # disease_path = "data/ot_files/diseases"
        # self.disease_df = self.spark.read.parquet(disease_path)
        #
        # go_path = "data/ot_files/go"
        # self.go_df = self.spark.read.parquet(go_path)
        #
        # mp_path = "data/ot_files/mousePhenotypes"
        # self.mp_df = self.spark.read.parquet(mp_path)

        if stats:

            # print schema
            print(self.drugs.printSchema())
            # print(self.target_df.printSchema())
            # print(self.disease_df.printSchema())
            # print(self.go_df.printSchema())
            # print(self.mp_df.printSchema())

            # print number of rows
            print(
                f"Length of evidence data: {self.drugs.count()} entries"
            )

            # print number of rows per datasource
            # self.evidence_df.groupBy("datasourceId").count().show(100)

        # if show_edges:
        #     for dataset in [field.value for field in self.datasets]:
        #         self.evidence_df.where(
        #             self.evidence_df.datasourceId == dataset
        #         ).show(1, 50, True)

        if show_nodes:
            self.drugs.show(1, 50, True)
            # self.disease_df.show(1, 50, True)
            # self.go_df.show(1, 50, True)
            # self.mp_df.show(1, 50, True)

    def show_datasources(self):
        """
        Utility function to get all datasources in the evidence data.
        """

        # collect all distinct datasourceId values
        datasources = (
            self.evidence_df.select("datasourceId").distinct().collect()
        )

        # convert to list
        self.datasources = [x.datasourceId for x in datasources]
        print(self.datasources)

    def _yield_drugs(
        self,
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

        for row in tqdm(self.drugs.collect()):

            print(row)

            # normalize id
            _id, _type = _process_id_and_type(
                row[node_field_type._PRIMARY_ID.value], ontology_class
            )

            # # switch mouse gene type
            # if node_field_type == MouseTargetNodeField:
            #     _type = "drug"

            if not _id:
                continue

            _props = {}
            _props["version"] = "22.11"
            _props["source"] = "NeDRex"
            _props["licence"] = "https://api.nedrex.net/static/licence"

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
        yield from self._yield_drugs(
           DrugNodeField
        )

        # Diseases
        # yield from self._yield_node_type(self.disease_df, DiseaseNodeField)
        #
        # # Gene Ontology
        # yield from self._yield_node_type(self.go_df, GeneOntologyNodeField)
        #
        # # Mouse Phenotypes
        # only_mp_df = self.mp_df.select(
        #     [field.value for field in MousePhenotypeNodeField]
        # ).dropDuplicates()
        # yield from self._yield_node_type(only_mp_df, MousePhenotypeNodeField)
        #
        # # Mouse Targets
        # mouse_target_df = self.mp_df.select(
        #     [field.value for field in MouseTargetNodeField]
        # ).dropDuplicates()
        # yield from self._yield_node_type(
        #     mouse_target_df, MouseTargetNodeField, "ensembl"
        # )

    def get_edge_batches(self):
        """
        Create a column with partition number in the evidence dataframe and
        return a list of batch numbers.
        """

        logger.info("Generating batches.")

        # select columns of interest
        self.evidence_df = self.evidence_df.where(
            self.evidence_df.datasourceId.isin(
                [field.value for field in self.datasets]
            )
        ).select([field.value for field in self.edge_fields])

        # add partition number to self.evidence_df as column
        self.evidence_df = self.evidence_df.withColumn(
            "partition_num", F.spark_partition_id()
        )
        self.evidence_df.persist()

        self.batches = [
            int(row.partition_num)
            for row in self.evidence_df.select("partition_num")
            .distinct()
            .collect()
        ]

        logger.info(f"Generated {len(self.batches)} batches.")

        return self.batches

    def get_edges(self, batch_number: int):
        """
        Yield edges from the evidence dataframe per batch.
        """

        # Check if self.evidence_df has column partition_num
        if "partition_num" not in self.evidence_df.columns:
            raise ValueError(
                "self.evidence_df does not have column partition_num. "
                "Please run get_edge_batches() first."
            )

        logger.info("Generating edges.")

        logger.info(
            f"Processing batch {batch_number+1} of {len(self.batches)}."
        )

        yield from self._process_edges(
            self.evidence_df.where(
                self.evidence_df.partition_num == batch_number
            )
        )

    def _process_edges(self, batch):
        """
        Process one batch of edges.

        Args:

            batch: Spark DataFrame containing the edges of one batch.
        """

        logger.info(f"Batch size: {batch.count()} edges.")

        if self.test_mode:
            # limit batch df to 100 rows
            batch = batch.limit(100)

        # yield edges per row of edge_df, skipping null values
        for row in tqdm(batch.collect()):

            # collect properties from fields, skipping null values
            properties = {}
            # for field in self.edge_fields:
                # skip disease and target ids, relationship id, and datatype id
                # as they are encoded in the relationship
                # if field not in [
                #     TargetDiseaseEdgeField.LITERATURE,
                #     TargetDiseaseEdgeField.SCORE,
                #     TargetDiseaseEdgeField.SOURCE,
                # ]:
                #     continue
                #
                # if field == TargetDiseaseEdgeField.SOURCE:
                #     properties["source"] = row[field.value]
                #     properties["licence"] = _find_licence(row[field.value])
                # elif row[field.value]:
                #     properties[field.value] = row[field.value]

            properties["version"] = "22.11"

            disease_id, _ = _process_id_and_type(row.diseaseId)
            # gene_id, _ = _process_id_and_type(row.targetId, "ensembl")

            yield (
                row.id,
                # gene_id,
                disease_id,
                row.datatypeId,
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

        _id = normalize_curie(f"{_type}:{inputId}")

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
