import subprocess
import os

import apache_beam as beam


class MyFilter(beam.DoFn):
    """
    Delay-free flight filter
    """
    def process(self, record, *args, **kwargs):
        if int(record[8]) > 0:
            return [record]


if __name__ == '__main__':

    # Caminho local do arquivo de dataset
    data_path = os.path.join(os.path.dirname(__file__), 'dataset/voos_sample.csv')

    if not os.path.exists(data_path):
        # Download dataset
        subprocess.run(f"wget --directory-prefix={os.path.dirname(data_path)} https://raw.githubusercontent.com/cassiobolba/Python/master/"
                       "Python-Apache-Beam/voos_sample.csv", shell=True)

    # Definindo pipeline
    p1 = beam.Pipeline()

    # beam.CombinePerKey
    time_delay = (
            p1
            | "Importar Dados Atraso" >> beam.io.ReadFromText(data_path, skip_header_lines=1)
            | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
            # voos com atraso
            | "Pegar voos de Los Angeles com atraso" >> beam.ParDo(MyFilter())
            # cria um dicionário
            | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
            # somas dos valores de atrasos (acúmulo de atrasos)
            | "Somar por key" >> beam.CombinePerKey(sum)
        # |   "Mostrar Resultados" >> beam.Map(print)
    )

    # beam.combiners.Count.PerKey
    number_of_delays = (
            p1
            | "Importar Dados" >> beam.io.ReadFromText(data_path, skip_header_lines=1)
            | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
            # voos com atraso
            | "Pegar voos de Los Angeles qtd" >> beam.ParDo(MyFilter())
            # cria um dicionário
            | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
            # contando quantos atrasos tem cada aeroporto
            | "Contar por key" >> beam.combiners.Count.PerKey()
        # |   "Mostrar Resultados" >> beam.Map(print)
    )

    delay_table = (
        # montando um dicionário com os resultados das duas pCollection acima
        {'number_of_delays': number_of_delays, 'time_delay': time_delay}
        # agrupando as duas pCollection em função de suas keys (semelhante a
        # operação JOIN do SQL)
        | "Group By" >> beam.CoGroupByKey()
        | beam.Map(print)
    )

    p1.run()
