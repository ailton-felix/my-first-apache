"""
A `DataflowRunner` example that submit the pipeline to a Dataflow compute instance.
This instance can be a single virtual machine. This scritp don't run the pipeline, just
create the template on GCS.
"""

import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# GCP authentication
service_account_path = './service_account.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path
# bucket path to save the final file
bucket_name = 'name_of_bucket'

pipeline_options = {
    'project': 'my-first-apache',   # The project ID for your Google Cloud Project.
    'runner': 'DataflowRunner',  # The pipeline runner to use.
                                 # 'DataflowRunner' run the pipeline on the Cloud Dataflow Service.
                                 # 'DirectRunner' run the pipeline on local machine
    'region': 'us-east-1',  # The Google Compute Engine region to create the job.
    'staging_location': f'gs://{bucket_name}/staging_dir',  # Optional. GCS bucket path for staging your binary and any temporary files
    'temp_location': f'gs://{bucket_name}/temp_dir',  # Required. Path for temporary files. A valid GCS URL that begins with gs://.
    'template_location': f'gs://{bucket_name}/template/template_name'
}

class MyFilter(beam.DoFn):
    """
    Delay-free flight filter
    """
    def process(self, record, *args, **kwargs):
        if int(record[8]) > 0:
            return [record]


if __name__ == '__main__':

    # Storage path to the dataset file
    data_path = os.path.join('gs://', bucket_name, 'input/flights_sample.csv')

    pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
    # Define pipeline
    p1 = beam.Pipeline(options=pipeline_options)

    # beam.CombinePerKey
    time_delay = (
            p1
            | "Importar Dados Atraso" >> beam.io.ReadFromText(data_path, skip_header_lines=1)
            | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
            # flights late
            | "Pegar voos de Los Angeles com atraso" >> beam.ParDo(MyFilter())
            # create a dictionary
            | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
            # sum of delayed flights values (accumulation of delays)
            | "Somar por key" >> beam.CombinePerKey(sum)
        # |   "Mostrar Resultados" >> beam.Map(print)
    )

    # beam.combiners.Count.PerKey
    number_of_delays = (
            p1
            | "Importar Dados" >> beam.io.ReadFromText(data_path, skip_header_lines=1)
            | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
            # flights late
            | "Pegar voos de Los Angeles qtd" >> beam.ParDo(MyFilter())
            # create a dictionary
            | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
            # count how many delays each airport has
            | "Contar por key" >> beam.combiners.Count.PerKey()
        # |   "Mostrar Resultados" >> beam.Map(print)
    )

    delay_table = (
        # builds a dictionary with the two results from the pCollection above
            {'number_of_delays': number_of_delays, 'time_delay': time_delay}
            # agrupando as duas pCollection em função de suas keys (semelhante a
            # operação JOIN do SQL)
            | "Group By" >> beam.CoGroupByKey()
            | 'Saída para GCP storage' >> beam.io.WriteToText(f'gs://{bucket_name}/output/dalayed_flights.csv')
    )

    print('Creating dataflow template...')
    p1.run()

    print('Operation Successful')
