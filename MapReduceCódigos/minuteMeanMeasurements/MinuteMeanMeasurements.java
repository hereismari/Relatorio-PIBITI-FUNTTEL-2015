/* Importando bibliotecas referentes ao Hadoop */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/* Importando outras bibliotecas necessárias nessa aplicação em específico*/
import java.text.*;
import java.io.IOException;
import java.util.*;

/* Classe principal que contém uma classe que fará o papel de Mapper
   e uma classe que fará o papel de Reducer.
   Nesta aplicação o resultado final será a média das medições por minuto.
*/
public class MinuteMeanMeasurements {

	/* 
	   Classe que representa o Map, herda de MapReduceBase e implementa um Mapper<tipo_da_chave_de_entrada, tipo_do_valor_de_entrada,
	   tipo_da_chave_de_saída, tipo_do_valor_de_saída>.
	   Os tipos utilizados foram os disponíveis em org.apache.hadoop.io.*
	*/
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text sensor;
		private Text word = new Text();

		/*
		    Método map: recebe a chave e tipo dos valores especificados na classe Map, um Coletor<tipo_da_chave_de_saída, tipo_do_valor_de_saída>
		    e um Reporter. Perceba que o método não tem retorno, no fim do map a mensagem será coletada.
		*/
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			/* Em cada map iremos receber uma mensagem/linha e processá-la para retirar a informação requerida */
			String line = value.toString();
			String[] parts = line.split(";");

			/* Separamos o id da mensagem, e convertemos o tempo da mensagem de unix para uma formatação mais visual(hora:minuto) */
			String id = parts[0];
			long unix = Long.parseLong(parts[2], 10);
			Date date = new Date(unix);
			SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
			sdf.setTimeZone(TimeZone.getTimeZone("GMT-3"));
			String hour = sdf.format(date);
			
			/* A saída que será coletada será algo:
				chave: id e hora:minuto que mensagem foi enviada pelo sensor
				valor: quantas vezes vimos a mensagem(1)
				Sendo a chave e valor dos tipos especificados na classe.
			*/
			sensor = new Text(parts[3]);
			word.set(hour);
			output.collect(word, sensor);
		}
	}
	

	/* 
	   Classe que representa o Combiner, herda de MapReduceBase e implementa um Mapper<tipo_da_chave_de_entrada, tipo_do_valor_de_entrada,
	   tipo_da_chave_de_saída, tipo_do_valor_de_saída>.
	   Os tipos utilizados foram os disponíveis em org.apache.hadoop.io.*
	   Lembrando que no Combiner os tipos de entrada devem ser os mesmos que os de saída.
	*/
	public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text > {

		/*
		    Método reduce: recebe a chave e um iterator dos tipo especificados na classe Reduce, um Coletor<tipo_da_chave_de_saída, tipo_do_valor_de_saída>
		    e um Reporter. Perceba que o método não tem retorno, no fim do reduce a mensagem será coletada.
		*/
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text > output, Reporter reporter)
				throws IOException {

			/* Neste Combiner manteremos a chave recebida, porém emiteremos como valor tanto a soma como
			   quantas medições contamos, assim a média não será alterada.
			*/
			Double sum = 0D;
			Integer count = 0;
			while (values.hasNext()) {
				sum += Double.parseDouble(values.next().toString());
				count += 1;
			}
		/*A chave e valor coletados serão respectivamente a chave e um dos valores da lista de valores da entrada do Reduce */
            Double average = sum / count;
			output.collect(key, new Text(average.toString() + '_' + count.toString()));
		}
	}

	/* 
	   Classe que representa o Reduce, herda de MapReduceBase e implementa um Reducer<tipo_da_chave_de_entrada, tipo_da_lista_de_valores_de_entrada,
	   tipo_da_chave_de_saída, tipo_do_valor_de_saída>.
	   Os tipos utilizados foram os disponíveis em org.apache.hadoop.io.*
	*/
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, DoubleWritable> {

		/*
		    Método reduce: recebe a chave e um iterator dos tipo especificados na classe Reduce, um Coletor<tipo_da_chave_de_saída, tipo_do_valor_de_saída>
		    e um Reporter. Perceba que o método não tem retorno, no fim do reduce a mensagem será coletada.
		*/
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			/* Como estamos interessados na média da chave
			   iteramos no valor do iterator acumulando o valor e contando as ocorrências para
			   obtermos a média, e depois coletarmos com os tipos especificados na classe Reduce.
			*/
			double sum = 0;
			int total_count = 0;
			while (values.hasNext()) {
                String text = values.next().toString();
                String[] tokens = text.split("_");
                Double average = Double.parseDouble(tokens[0]);
                Integer count = Integer.parseInt(tokens[1]);
                sum += (average * count);
                total_count += count;
			}

            double average = sum / total_count;
			output.collect(key, new DoubleWritable(average));
		}
	}

	/* Configuração do Job para utilização das classes feitas acima */
	public static void main(String[] args) throws Exception {
		
		/* Criando um novo JobConf com a classe principal que criamos */
		JobConf conf = new JobConf(MinuteMeanMeasurements.class);
		conf.setJobName("MinuteMeanMeasurements");
		
		/* Configuração referente ao que será coletado no Reduce */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		/* Configuração referente ao que será coletado no Map */
		conf.setMapOutputKeyClass(Text.class); 
  		conf.setMapOutputValueClass(Text.class); 

		/* Configuração do Mapper, Combiner e Reducer. Neste caso o Combiner foi criado */
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);

		/* Configuração referente aos tipos de arquivo de entrada e saída, neste caso arquivos de texto */
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		/* Adiciona um caminho a lista de entradas do job map-reduce */
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		/* Adiciona um caminho a lista de saídas do job map-reduce */
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		/* Execução do job */
		JobClient.runJob(conf);

	}
}
