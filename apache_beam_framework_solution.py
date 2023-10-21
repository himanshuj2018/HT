import csv
import apache_beam as beam

# Define a custom Function to read CSV files
class ReadCSVFile(beam.DoFn):
    """ Read and yield data from a CSV file.

        Parameters:
        - element (str): The path to the CSV file to read.

        Yields:
        - dict: Each row of data from the CSV file as a dictionary.
    """
    def process(self, element):
        csv_file = element
        with open(csv_file, 'r', newline='') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                yield (row)

class ReshapeData(beam.DoFn):
    """Reshape data by adding 'tier' from dataset2 to dataset1 entries post combining the data.

        Parameters:
        - element (tuple): A tuple containing data from dataset1 and dataset2.

        Yields:
        - dict: Reshaped entries with 'tier' added.
        """
    def process(self, element):
        _, data = element
        dataset1 = data['dataset1']
        dataset2 = data['dataset2']

        for entry in dataset1:
            entry_copy = entry.copy()
            entry_copy['tier'] = dataset2[0]['tier']
            yield entry_copy

class CompositeKeyFn(beam.DoFn):
    """ Create composite keys for grouping.

        Parameters:
        - element (dict): A data element.
        - keys (list): List of keys to use for composing the composite key.

        Yields:
        - tuple: A tuple with a composite key and the input element.
    """
    def process(self, element, keys):
        composite_key = "-".join([element[key] for key in keys])
        yield (composite_key, element)


class ExtractAndSum(beam.DoFn):
    """ Perform aggregation on grouped data.

        Parameters:
        - element (tuple): A tuple with a composite key and a group of data elements.

        Yields:
        - tuple: Aggregated values for the group, including max rating, sum of 'ARAP' values, sum of 'ACCR' values, and the total count.
    """
    def process(self, element):
        key, data = element
        max_rating = max(int(x['rating']) for x in data)
        sum_arap = sum(int(x['value']) for x in data if x['status'] == "ARAP")
        sum_accr = sum(int(x['value']) for x in data if x['status'] == "ACCR")
        total = len(data)
        yield (key, max_rating, sum_arap, sum_accr, total)

class PrepareOutputData(beam.DoFn):
    """ Prepare the output data.

        Parameters:
        - element (tuple): A tuple with aggregated values.
        - group_by_key (list): List of keys for grouping.

        Yields:
        - dict: The prepared output data with appropriate column names.
    """
    def process(self, element, group_by_key):
        aggr_columns =  ['legal_entity', 'counter_party', 'tier']
        key, max_rating, sum_arap, sum_accr, total = element
        split_key = key.split("-")

        result_dict = {}
        for i, key in enumerate(group_by_key):
            result_dict[key] = split_key[i]

        for column in aggr_columns:
            if column not in result_dict:
                result_dict[column] = total

        result_dict["max(rating by counterparty)"] = max_rating   
        result_dict["sum(value where status=ARAP)"] = sum_arap
        result_dict["sum(value where status=ACCR)"] = sum_accr
        
        yield result_dict

class SaveOutputToCsv(beam.DoFn):
    def __init__(self, output_file):
        self.output_file = output_file

    def process(self, element):
        """ Save the output data to a CSV file in a specific format.

        Parameters:
        - element: The data to save to the output file.

        Yields:
        - str: The path to the output file.
        """
        headers = ["legal_entity", "counter_party", "tier", "max(rating by counterparty)", 
                   "sum(value where status=ARAP)", "sum(value where status=ACCR)"]
        
        with open(self.output_file, 'a', newline='') as file: 
            writer = csv.DictWriter(file, fieldnames=headers)
            if file.tell() == 0:
                writer.writeheader()  # Write header only if the file is empty
            for row in element:
                # Assuming the input data is a dictionary
                row_to_write = {
                    "legal_entity": row["legal_entity"],
                    "counter_party": row["counter_party"],
                    "tier": row["tier"],
                    "max(rating by counterparty)": row["max(rating by counterparty)"],
                    "sum(value where status=ARAP)": row["sum(value where status=ARAP)"],
                    "sum(value where status=ACCR)": row["sum(value where status=ACCR)"]
                }
                writer.writerow(row_to_write)
        yield self.output_file

if __name__ == "__main__":
    # Define a list of group_by_columns that encompasses all possible combinations of 1, 2, and 3 columns at a time.
    group_by_columns = [
         ['tier'], ['counter_party'], ['legal_entity'],
         ['legal_entity', 'counter_party'], ['counter_party', 'tier'], ['tier', 'legal_entity'],
        ['legal_entity', 'counter_party', 'tier']
    ]
    
    with beam.Pipeline() as p:
        # Read and process dataset1
        dataset1 = (
            p
            | "Read Dataset1" >> beam.Create(["dataset1.csv"])  
            | "Read CSV File1" >> beam.ParDo(ReadCSVFile()) 
            | "Map to key-value1" >> beam.Map(lambda cols: (cols["counter_party"], cols)) 
        )
        
        # Read and process dataset2
        dataset2 = (
            p
            | "Read Dataset2" >> beam.Create(["dataset2.csv"])  
            | "Read CSV File2" >> beam.ParDo(ReadCSVFile()) 
            | "Map to key-value2" >> beam.Map(lambda cols: (cols["counter_party"], cols)) # This is Mapped so that we can merge data later on based on counter_party
        )
        
        # Merge the data from dataset1 and dataset2 based on 'counter_party'
        merged_data = (
            ({"dataset1": dataset1, "dataset2": dataset2}) 
            | "Merging" >> beam.CoGroupByKey() # Both dataset are combined based on the common key 'counter_party' mapped earlier
            | "Reshape Data" >> beam.ParDo(ReshapeData())
        )
        
        # Apache Beam is designed for parallel processes so the data written will not be in order
        # Process data for each group_by_key
        for group_by_key in group_by_columns:
            grouped_data = (
                merged_data 
                # Composite key is being created to handle grouping of data on multiple keys e.g. ['legal_entity', 'counter_party']
                | f"Create Composite Key {group_by_key}" >> beam.ParDo(CompositeKeyFn(), group_by_key) 
                | f"Group by Composite Key {group_by_key}" >> beam.GroupBy(lambda x: x[0]) # Group the dict elements by different keys of 1,2 and 3 combinations at a time  
                | f"Remove extra composite key Element {group_by_key}" >> beam.Map(lambda group: (group[0], [x[1] for x in group[1]] ))
                | f"Perform Aggregations {group_by_key}" >> beam.ParDo(ExtractAndSum())
                | f"Prepare Output Data {group_by_key}" >> beam.ParDo(PrepareOutputData(), group_by_key) 
                | f"Collect Output Data {group_by_key}" >> beam.combiners.ToList()
                | f"Save Output to CSV {group_by_key}" >> beam.ParDo(SaveOutputToCsv("apache_beam_framework_output.csv"))
            )
