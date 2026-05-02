# sensor-data-pthreads

Software com pthreads para análise paralela de dados JSON contendo temperatura, umidade, bateria e pressão. Os dados foram coletados nas cidades de Caxias do Sul e Bento Gonçalves por sensores do projeto CityLivingLab.

Linguagem: C

Biblioteca de Parsing: cJSON

gcc main.c cJSON.c -o analisador -lm -pthread

./analisador
