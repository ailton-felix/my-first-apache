import subprocess
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# GCP authentication
service_account_path = './service_account.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path
# bucket path to save the final file
bucket_name = 'name_of_bucket'

pipeline_options = {
    'project': 'my-first-apache',
    'runner': 'DataflowRunner',
    'region': 'us-east-1',
    'staging_location': 'gs://my-staging',
    'temp_location': 'gs://my-temp',
    'template_location': 'gs://my-temp'
}

class MyFilter(beam.DoFn):
    """
    Delay-free flight filter
    """
    def process(self, record, *args, **kwargs):
        if int(record[8]) > 0:
            return [record]


if __name__ == '__main__':

    # Caminho local do arquivo de dataset
    # Local path to the dataset file
    data_path = os.path.join(os.path.dirname(__file__), 'dataset/voos_sample.csv')

    if not os.path.exists(data_path):
        # Download dataset
        subprocess.run(f"wget --directory-prefix={os.path.dirname(data_path)} https://raw.githubusercontent.com/cassiobolba/Python/master/"
                       "Python-Apache-Beam/voos_sample.csv", shell=True)

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
            | 'Saída para GCP storage' >> beam.io.WriteToText(f'gs://{bucket_name}/dalayed_flights.csv')
    )

    p1.run()
