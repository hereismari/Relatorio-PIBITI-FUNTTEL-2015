#medições por minuto
data = read.table('<path>/part-00000')
img_medicoes_por_minuto = ggplot(data,aes(V2, V3, fill=V1)) + geom_bar(position="dodge",stat="identity") + xlab("hora do dia") + ylab("quantidade de medições") + labs(title="Quantidade de medições por minuto em um dia") + scale_x_discrete(breaks=c("00:00", "02:00", "04:00", "06:00", "08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00")) + scale_fill_discrete(name = "Sensores", labels = c("sensor 1","sensor 2")) 

#medições por hora
data = read.table('<path>/part-00000')
img_medicoes_por_hora = ggplot(data,aes(V2, V3, fill=V1)) + geom_bar(position="dodge",stat="identity") + xlab("hora do dia") + ylab("quantidade de medições") + labs(title="Quantidade de medições por hora em um dia") + scale_fill_discrete(name = "Sensores", labels = c("sensor 1","sensor 2")) 

#média de medições por minuto
data = read.table('<path>/part-00000')
img_media_das_medicoes = ggplot(data,aes(V1, V2)) + geom_bar(position="dodge",stat="identity", fill="blue") + labs(title="Média das medições por minuto em um dia", x = "minuto do dia", y = "média das medições") + scale_fill_discrete(name = "ID dos sensores") + scale_x_discrete(breaks=c("00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"))
