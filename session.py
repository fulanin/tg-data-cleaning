class Session:
  def __init__(self, cleaner):
    self.cleaner = cleaner
    self.dataset = Dataset()
    self.parser  = Parser(self)
    self.graph = Graph()
    self.error_detector = ErrorDetector(self)
    self.dcs = [] # Denial constraints string
    self.dc_objects = {}  # Denial Constraint Objects

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
    self.error_detector.get_noisy_cells

  def repair(self):
    init_table_name  = "init_"      + self.dataset.id
    dk_table_name    = "C_dk_temp_" + self.dataset.id
    clean_table_name = "C_clean_"   + self.dataset.id

    # Creates a graph using the dk table
    tuple_rows = self.cleaner.postgres.get_tuple_rows(dk_table_name)
    graph.create_graph(tuple_rows)

    # Creates a clean table that initially is a copy of the init table
    create_table_query = "CREATE TABLE " + clean_table_name + " AS TABLE " + init_table_name + ";" 
    self.cleaner.postgres.execute_query(create_table_query)

    # Removes vertexes from the graph until there are no more edges
    while len(self.graph.edges) > 0:
      it = 0
      for node in self.edges:
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