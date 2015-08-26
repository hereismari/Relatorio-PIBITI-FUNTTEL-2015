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
*/
public class MessageCounterPerMinute {

	/* 
	   Classe que representa o Map, herda de MapReduceBase e implementa um Mapper<tipo_da_chave_de_entrada, tipo_do_valor_de_entrada,
	   tipo_da_chave_de_saída, tipo_do_valor_de_saída>.
	   Os tipos utilizados foram os disponíveis em org.apache.hadoop.io.*
	*/
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		    Método map: recebe a chave e tipo dos valores especificados na classe Map, um Coletor<tipo_da_chave_de_saída, tipo_do_valor_de_saída>
		    e um Reporter. Perceba que o método não tem retorno, no fim do map a mensagem será coletada.
		*/
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			/* Em cada map iremos receber uma mensagem e processá-la para retirar a informação requerida */
			String line = value.toString();
			String[] parts = line.split(";");

			/* Separamos o id da mensagem, e convertemos o tempo da mensagem de unix para uma formatação mais visual hora:minuto*/
			String id = parts[0];
			long unix = Long.parseLong(parts[2], 10);
			Date date = new Date(unix);
			SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
			sdf.setTimeZone(TimeZone.getTimeZone("GMT-3"));
			String hour_and_minute = sdf.format(date);
			
			/* A saída que será coletada será algo:
				chave: id hora:minuto que mensagem foi enviada pelo sensor
				valor: quantas vezes vimos a mensagem(1) 
				Sendo a chave e valor dos tipos especificados na classe.
			*/
			word.set(id + " " + hour_and_minute);
			output.collect(word,one);
		}
	}
	
	/* 
	   Classe que representa o Reduce, herda de MapReduceBase e implementa um Reducer<tipo_da_chave_de_entrada, tipo_da_lista_de_valores_de_entrada,
	   tipo_da_chave_de_saída, tipo_do_valor_de_saída>.
	   Os tipos utilizados foram os disponíveis em org.apache.hadoop.io.*
	*/
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		/*
		    Método reduce: recebe a chave e um iterator dos tipo especificados na classe Reduce, um Coletor<tipo_da_chave_de_saída, tipo_do_valor_de_saída>
		    e um Reporter. Perceba que o método não tem retorno, no fim do reduce a mensagem será coletada.
		*/
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			/* Como estamos interessados no número de ocorrência das mensagems dessa chave específica
			   iteramos no valor do iterator acumulando o valor, para depois coletarmos com os tipos especificados na classe Reduce.
			*/
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			output.collect(key, new IntWritable(sum));
		}
	}

	/* Configuração do Job para utilização das classes feitas acima */
	public static void main(String[] args) throws Exception {
		
		/* Criando um novo JobConf com a classe principal que criamos */
		JobConf conf = new JobConf(MessageCounterPerMinute.class);
		conf.setJobName("MessageCounterPerHour");
		
		/* Configuração referente ao que será coletado no Reduce */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		/* Configuração referente ao que será coletado no Map */
		conf.setMapOutputKeyClass(Text.class); 
  		conf.setMapOutputValueClass(IntWritable.class); 


		/* Configuração do Mapper, Combiner e Reducer. Neste caso o Combiner é a própria classe Reduce */
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
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
