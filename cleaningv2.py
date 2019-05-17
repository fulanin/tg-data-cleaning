from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, DoubleType
from collections import defaultdict

import psycopg2
import random
import unittest

class DCFormatException(Exception):
    pass

class TestRepair:
  def __init__(self):
    self.dataset_path = "data/hospital.csv"
    self.dcs_path     = "data/hospital_constraints.txt"
    self.cleaner = Cleaner()
    self.session = Session(self.cleaner)

    self.dataset = self.session.dataset
    self.parser = self.session.parser
    self.dc_objects = self.session.dc_objects

  def execute(self):
    self.session.ingest_dataset(self.dataset_path)
    self.session.ingest_dcs(self.dcs_path)
    self.session.detect_errors()
    self.session.repair()

  def test(self):
    for dc_index, dc_name in enumerate(self.dc_objects):
      dc_object = self.dc_objects[dc_name]
      tmp_table = "tmp_" + self.dataset.id + "_" + str(dc_index)

      create_table_query = "CREATE TABLE " + tmp_table + " AS SELECT "
      for tuple_name in dc_object.tuple_names:
        create_table_query += tuple_name + ".id as " + tuple_name + "_id,"
      create_table_query  = create_table_query[:-1]
      create_table_query += " FROM  "

      for tuple_name in dc_object.tuple_names:
        create_table_query += "c_clean_" + self.dataset.id + " as " + tuple_name + ","
      create_table_query  = create_table_query[:-1]
      create_table_query += " WHERE "
      create_table_query += dc_object.tuple_names[0] + " != " + dc_object.tuple_names[1] + \
                            " AND " + dc_object.cnf_form
      # print create_table_query
      self.cleaner.postgres.execute_query(create_table_query)

      tuple_rows = self.cleaner.postgres.get_tuple_rows(tmp_table)
      print "\n"
      print "The DC " + dc_name + " is violated " + str(len(tuple_rows)) + " times."
      print "Expected violations: 0"

      drop_table_query = "DROP TABLE " + tmp_table + ";"
      self.cleaner.postgres.execute_query(drop_table_query)

class ErrorDetector:
  def __init__(self, session):
    self.session = session
    self.cleaner = session.cleaner
    self.dataset = session.dataset
    self.parser = session.parser
    self.dc_objects = session.dc_objects

  def get_noisy_cells(self):
    table_name = "C_dk_temp_" + self.dataset.id
    create_table_query = "CREATE TABLE " + table_name + "(t1_id INT, t2_id INT, violated_dc VARCHAR(255));"
    self.cleaner.postgres.execute_query(create_table_query)

    for dc_index, dc_name in enumerate(self.dc_objects):
      # print "\n" + dc_name

      # self._get_noisy_cells_for_dc(dc_name)
      dc_object = self.dc_objects[dc_name]
      tmp_table = "tmp_" + self.dataset.id

      create_table_query = "CREATE TABLE " + tmp_table + " AS SELECT "
      for tuple_name in dc_object.tuple_names:
        create_table_query += tuple_name + ".id as " + tuple_name + "_id,"
      create_table_query = create_table_query[:-1]
      create_table_query += " FROM  "

      for tuple_name in dc_object.tuple_names:
        create_table_query += "init_" + self.dataset.id + " as " + tuple_name + ","
      create_table_query = create_table_query[:-1]
      create_table_query += " WHERE "
      create_table_query += dc_object.tuple_names[0] + " != " + dc_object.tuple_names[1] + \
                            " AND " + dc_object.cnf_form
      # print create_table_query
      self.cleaner.postgres.execute_query(create_table_query)

      alter_table_query  = "ALTER TABLE " + tmp_table
      alter_table_query += " ADD COLUMN violated_dc VARCHAR(255) DEFAULT '" + dc_name.replace("'", "\"") + "';"
      self.cleaner.postgres.execute_query(alter_table_query)    

      insert_into_query =  "INSERT INTO " + table_name + "(t1_id, t2_id, violated_dc)"
      insert_into_query += "SELECT * FROM " + tmp_table + ";"
      self.cleaner.postgres.execute_query(insert_into_query)

      drop_table_query = "DROP TABLE " + tmp_table + ";"
      self.cleaner.postgres.execute_query(drop_table_query)

      create_table_query = ""
            

class Parser:
  """
    This class creates interface for parsing denial constraints
  """
  def __init__(self, session):
    self.session = session
    # self.dataengine = session.holo_env.dataengine

  def load_dcs(self, file_path, all_current_dcs):
    """
      Loads the Denial Constraints from the line separated .txt file

      :param file_path: path to dc file
      :param all_current_dcs: list of current dcs in the session

      :return: string array of dc's
    """

    # Array of strings containing the dc's
    dcs_strings = []

    # Strings containing the dc's
    dcs = {}
    dc_file = open(file_path, 'r')
    for line in dc_file:
      if not line.isspace():
        line = line.rstrip()
        if line in all_current_dcs:
          raise DCFormatException('DC already added')
        dcs_strings.append(line)
        dcs[line] = DenialConstraint(line, self.session.dataset.attributes['Init'])

    return dcs_strings, dcs

class Predicate:
  def __init__(self, predicate_string, tuple_names, schema):
    """
      :param predicate_string: string shows the predicate
      :param tuple_names: name of tuples in denial constraint
      :param schema: list of attributes
    """

    op_index = DenialConstraint.contains_operation(predicate_string)
    if op_index is not None:
      self.operation_string = DenialConstraint.operationSign[op_index]
      self.operation = DenialConstraint.operationsArr[op_index]
    else:
      raise DCFormatException('Cannot find Operation in Predicate: ' + predicate_string)

    self.tuple_names = tuple_names
    self.schema = schema
    self.cnf_form = ""
    self.components = self.parse_components(predicate_string)
 
    for i in range(len(self.components)):
      component = self.components[i]
      if isinstance(component, str):
        self.cnf_form += component
      else:
        self.cnf_form += component[0] + "." + component[1]
      if i < len(self.components) - 1:
        self.cnf_form += self.operation

    return

  def parse_components(self, predicate_string):
    """
      Parse the components of a given string

      :return: list of the components in the predicate
    """

    # This cleaning method only supports DCs with two tuples per predicate
    num_tuples = len(predicate_string.split(','))
    if num_tuples < 2:
      raise DCFormatException('Less than 2 tuples in predicate: ' +
                              predicate_string)
    elif num_tuples > 2:
      raise DCFormatException('More than 2 tuples in predicate: ' +
                              predicate_string)

    operation = self.operation_string
    if predicate_string[0:len(operation)] != operation:
      raise DCFormatException('First string in predicate is not an operation ' + predicate_string)
    stack = []
    components = []
    current_component = []
    str_so_far = ""

    for i in range(len(operation), len(predicate_string)):
      str_so_far += predicate_string[i]
      if len(stack[-1:]) > 0 and stack[-1] == "'":
        if predicate_string[i] == "'":
          if i == len(predicate_string) - 1 or predicate_string[i+1] != ')':
            raise DCFormatException("Expected ) after end of literal")
          components.append(str_so_far)
          current_component = []
          stack.pop()
          str_so_far = ""
      elif str_so_far == "'":
          stack.append("'")
      elif str_so_far == '(':
          str_so_far = ''
          stack.append('(')
      elif str_so_far == ')':
        if stack.pop() == '(':
          str_so_far = ''
          if len(stack) == 0:
            break
        else:
          raise DCFormatException('Closed an unopened (' + predicate_string)
      elif predicate_string[i + 1] == '.':
        if str_so_far in self.tuple_names:
          current_component.append(str_so_far)
          str_so_far = ""
        else:
          raise DCFormatException('Tuple name ' + str_so_far + ' not defined in ' + predicate_string)
      elif (predicate_string[i + 1] == ',' or
            predicate_string[i + 1] == ')') and \
            predicate_string[i] != "'":
        if str_so_far in self.schema:
          current_component.append(str_so_far)
          str_so_far = ""
          components.append(current_component)
          current_component = []
        else:
          raise DCFormatException('Attribute name ' + str_so_far + ' not in schema')
      elif str_so_far == ',' or str_so_far == '.':
        str_so_far = ''

    return components

class DenialConstraint:
  operationsArr = ['=', '<', '>', '<=', '>=', '<>']
  operationSign = ['EQ', 'LT', 'GT', 'LTE', 'GTE', 'IQ']

  def __init__(self, dc_string, schema):
    """
      Constructing denial constraint object

      :param dc_string: string for denial constraint
      :param schema: list of attribute
    """
    self.tuple_names = []
    self.predicates = []
    self.cnf_form = ""
    dc_string = dc_string.replace('"', "'")

    # Split is an array where each index is a string that represents dc component 
    split = dc_string.split('&')

    # Find all tuple names used  in DC
    for component in split:
      if DenialConstraint.contains_operation(component) is not None:
        break
      else:
        self.tuple_names.append(component)

    # Make a predicate for each component that's not a tuple name
    for i in range(len(self.tuple_names), len(split)):
      self.predicates.append(Predicate(split[i], self.tuple_names, schema))

    # Create CNF form of the DC
    cnf_forms = [predicate.cnf_form for predicate in self.predicates]
    self.cnf_form = " AND ".join(cnf_forms)
    return

  
  @staticmethod
  def contains_operation(string):
    """
    Method to check if a given string contains one of the operation signs

    :param string: given string

    :return: operation index in list of pre-defined list of operations or
    Null if string does not contain any
    """

    for i in range(len(DenialConstraint.operationSign)):
      if string.find(DenialConstraint.operationSign[i]) != -1:
        return i
    return None

class Cleaner:
  def __init__(self):
    self.pg_path = 'lib/postgresql-42.2.2.jar'
    self.spark_session, self.spark_sql_ctxt = self._init_spark()
    self.postgres = Postgres(self)

  def _init_spark(self):
    """
    Set spark configuration

    :return: Spark session
    :return: Spark context
    """
    conf = SparkConf()

    # Link PG driver to Spark
    conf.setAll([("spark.executor.extraClassPath", self.pg_path),
                 ("spark.driver.extraClassPath", self.pg_path),
                 ('spark.driver.memory', '2g'),
                 ('spark.executor.memory', '2g'),
                 ("spark.network.timeout", "6000"),
                 ("spark.rpc.askTimeout", "99999"),
                 ("spark.worker.timeout", "60000"),
                 ("spark.driver.maxResultSize", '50g'),
                 ("spark.ui.showConsoleProgress", "false")])

    # Gets Spark context
    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    sql_ctxt = SQLContext(sc)
    return sql_ctxt.sparkSession, sql_ctxt


class Session:
  def __init__(self, cleaner):
    self.dcs = [] # Denial constraints string
    self.dc_objects = {}  # Denial Constraint Objects
    self.cleaner = cleaner
    self.dataset = Dataset()
    self.parser  = Parser(self)
    self.error_detector = ErrorDetector(self)
    self.graph = Graph()

  def ingest_dataset(self, filepath):
    self.init_dataset, self.attribute_map = self.cleaner.postgres.ingest_dataset(filepath, self.dataset)
    all_attr = self.init_dataset.schema.names
    all_attr.remove("id")
    number_of_tuples = len(self.init_dataset.collect())
    tuples = [[i] for i in range(1, number_of_tuples + 1)]
    attr = [[a] for a in all_attr]
    tuples_dataframe = self.cleaner.spark_session.createDataFrame(tuples, ['id'])
    attr_dataframe = self.cleaner.spark_session.createDataFrame(attr, ['attr'])
    self.init_flat = tuples_dataframe.crossJoin(attr_dataframe)

    return

  def ingest_dcs(self, file_path):
    new_dcs, new_dc_objects = self.parser.load_dcs(file_path, self.dcs)
    self.dcs.extend(new_dcs)
    self.dc_objects.update(new_dc_objects)

    return self.dcs

  def detect_errors(self):
    self.error_detector.get_noisy_cells()

  def repair(self):
    init_table_name  = "init_"      + self.dataset.id
    dk_table_name    = "C_dk_temp_" + self.dataset.id
    clean_table_name = "C_clean_"   + self.dataset.id

    # Creates a graph using the dk table
    tuple_rows = self.cleaner.postgres.get_tuple_rows(dk_table_name)
    self.graph.create_graph(tuple_rows)

    # Creates a clean table that initially is a copy of the init table
    create_table_query = "CREATE TABLE " + clean_table_name + " AS TABLE " + init_table_name + ";" 
    self.cleaner.postgres.execute_query(create_table_query)

    # Removes vertexes from the graph until there are no more edges
    while len(self.graph.edges) > 0:
      it = 0
      for node in self.graph.edges:
        if (it == 0):
          largest = node
          it = 1
        if len(self.graph.edges[node]) > len(self.graph.edges[largest]):
          largest = node
      self.graph.remove_node(largest)
      delete_tuple_query = "DELETE FROM " + clean_table_name + \
                            " WHERE "      + clean_table_name + \
                            ".id = "       + str(largest)     + ";"
      self.cleaner.postgres.execute_query(delete_tuple_query)

    return

class Dataset:
  def __init__(self):
    self.id = self.id_generator()
    self.attribute = {}
    self.schema = ""
    self.attributes = {'id': [], 'Init': []}

  def id_generator(self):
    id = str(random.random())[2:]
    return id

class Reader:
  def __init__(self, spark_session):
    """
    Constructing reader object

    :param spark_session: The spark_session we created in Holoclean object
    """
    self.spark_session = spark_session

  def read(self, path):
    """
    Reads a .csv file that contains the dataset

    Returns the spark dataframe
    """
    dataframe = self.spark_session.read.csv(path, header=True)

    new_cols = dataframe.schema.names + ["id"]
    # print new_cols
    schema_list = []

    for i in range(len(dataframe.schema.names)):
      schema_list.append(StructField(str(i), dataframe.schema[i].dataType))
    schema_list.append(StructField(str(len(new_cols)), LongType()))

    schema = StructType(schema_list)
    ix_dataframe = dataframe.rdd.zipWithIndex().\
                   map(lambda (row, ix): row + (ix + 1,)).toDF(schema)
    tmp_cols = ix_dataframe.schema.names
    new_dataframe = reduce(lambda data, idx: data.withColumnRenamed(tmp_cols[idx],new_cols[idx]),
                           xrange(len(tmp_cols)), ix_dataframe)
    # Limpar strings maiores que 255 caracteres
    # new_df = self.checking_string_size(new_df)
    return new_dataframe

class Postgres:
  def __init__(self, cleaner):
    self.db_name = "cleaner"
    self.db_user = "cleaneruser"
    self.db_host = "localhost"
    self.db_pswd = "123456"

    self.cleaner = cleaner
    self.db_backend = self.init_db()
    self.sparkSqlUrl = self.init_sparksql_url()
    self.sql_ctxt = cleaner.spark_sql_ctxt

    self.attribute_map = {}

  def init_db(self):
    connect_string = "dbname= '" + self.db_name + "' user='" + self.db_user + \
                     "' host='" + self.db_host + "' password='" + self.db_pswd + "'"
    
    connector = psycopg2.connect(connect_string)
    cursor = connector.cursor()
    return cursor, connector

  def init_sparksql_url(self):
    jdbc_url = "jdbc:postgresql://" + self.db_host + "/" + self.db_name

    db_properties = {"user": self.db_user, "password": self.db_pswd, "ssl": "false", }
    return jdbc_url, db_properties

  def ingest_dataset(self, filepath, dataset):
    """
    Get a spark dataframe and creates a table on the db
    """

    new_reader = Reader(self.cleaner.spark_session)
    dataframe = new_reader.read(filepath)
    table_name = "init_" + dataset.id

    # dataframe_to_table
    self.create_table(dataframe, table_name) 

    dataframe.write.jdbc(self.sparkSqlUrl[0], table_name, "append", self.sparkSqlUrl[1])

    dataset.attributes['Init'] = dataframe.schema.names
    count = 0
    map_schema = []
    attribute_map = {}
    for attribute in dataframe.schema.names:
      if attribute != "id":
        count = count + 1
        map_schema.append([count, attribute])
        attribute_map[attribute] = count

    dataframe_map = self.cleaner.spark_session.createDataFrame(map_schema, 
                            StructType([
                              StructField("attr_id", IntegerType(), False),
                              StructField("attribute", StringType(), True)
                            ]))
    map_name = 'Map_schema_' + dataset.id
    self.create_table(dataframe_map, map_name)
    for table_tuple in map_schema:
      self.attribute_map[table_tuple[1]] = table_tuple[0]

    return dataframe, attribute_map
  
  def create_table(self, dataframe, table_name):
    # Dataframe to table
    # Create a string that representes the query for creating a table based on the spark dataframe
    jdbc_url = self.sparkSqlUrl
    
    create_table = "CREATE TABLE " + table_name + " ("
    for i in range(len(dataframe.schema.names)):
      create_table += dataframe.schema.names[i] + " "
      if (dataframe.schema.fields[i].dataType == IntegerType() or\
          dataframe.schema.names[i] == "id"):
        create_table += "INT,"
      elif (dataframe.schema.fields[i].dataType == DoubleType() or\
          dataframe.schema.names[i] == "id"):
        create_table += "DOUBLE PRECISION,"
      else:
        create_table += "VARCHAR (255),"
    if "id" in dataframe.schema.names:
      create_table += " PRIMARY KEY (id) "
    create_table = create_table[:-1] + " );"

    # The tables are created when the query is executed
    # To check the tables:
    #   $ sudo -u postgres psql
    #   \c cleaner
    #   \d  
    self.execute_query(create_table)
    # dataframe.write.jdbc(jdbc_url[0], table_name, "append", properties=jdbc_url[1])

    return

  def reset_database(self):
    drop_schema_query      = "DROP SCHEMA public CASCADE;"
    create_schema_query    = "CREATE SCHEMA public;"
    grant_privileges_query = "GRANT ALL PRIVILEGES on database " + \
                             self.db_name + " to " + self.db_user + ";"
    alter_schema_query     = "ALTER SCHEMA public OWNER TO " + self.db_user + ";"

    self.dataengine.query(drop_schema_query)
    self.dataengine.query(create_schema_query)
    self.dataengine.query(grant_privileges_query)
    self.dataengine.query(alter_schema_query)

  def execute_query(self, query):
    # print query
    self.db_backend[0].execute(query)
    self.db_backend[1].commit()

  def get_tuple_rows(self, table_name):
    query = "SELECT t1_id, t2_id FROM " + table_name + ";"
    self.db_backend[0].execute(query)
    rows = self.db_backend[0].fetchall()

    return rows

  def reset_database(self):
    query = "DROP SCHEMA public CASCADE;" +\
            "CREATE SCHEMA public;" +\
            "ALTER SCHEMA public OWNER TO cleaneruser;"
    self.execute_query(query)

class Graph:
  # {"a" : ["b", "c"], 
  #  "b" : ["a"], 
  #  "c" : ["a"]}
  # represents a graph with vertexes a, b, c 
  # with b, c connected to a
  def __init__(self, directed=False):
    self.edges = {}
    self.directed = directed

  def create_graph(self, tuple_rows):
    for row in tuple_rows:
      self.add_edge(row[0], row[1])
    
    return

  def add_edge(self, node_a=None, node_b=None):
    if (node_a is None) or (node_b is None):
      raise DCFormatException('Cannot add a NULL node')

    if (node_a in self.edges):
      if not (node_b in self.edges[node_a]):
        self.edges[node_a].append(node_b)
    else:
      self.edges[node_a] = []
      self.edges[node_a].append(node_b)
    if (node_b in self.edges):
      if not (node_a in self.edges[node_b]):
        self.edges[node_b].append(node_a)
    else:
      self.edges[node_b] = []
      self.edges[node_b].append(node_a)

    return

  def remove_node(self, node=None):
    if node is None:
      raise DCFormatException('Cannot remove a NULL node')

    if not (node in self.edges):
      raise DCFormatException('There is no node ' + node + ' in the graph')

    for neighbour in self.edges[node]:
      self.edges[neighbour].remove(node)
      if len(self.edges[neighbour]) == 0:
        del self.edges[neighbour]
    del self.edges[node]

    return

  def print_graph(self):
    print self.edges

    return
    


if __name__ == "__main__":
  new_test = TestRepair()
  new_test.execute()
  new_test.test()
  
  # new_graph.print_graph()
