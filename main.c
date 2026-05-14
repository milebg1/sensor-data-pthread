#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <float.h>
#include <unistd.h>
#include "cJSON.h"

#define BUFFER_SIZE 1000 // Tamanho da Fila

typedef struct {

    char cidade[50];

    double temperatura;
    double umidade;
    double pressao;
    double bateria;

    char datahora[50];
    char sf[20];

    long id;

} Registro; // Registros lidos dos arquivos JSON e processados para cálculo das estatísticas

typedef struct {

    Registro buffer[BUFFER_SIZE];

    int inicio;
    int fim;
    int quantidade;

    pthread_mutex_t mutex; // Mutex para controle de acesso à fila

    pthread_cond_t cheio;  // Condição para indicar que a fila está cheia e a thread de leitura deve esperar
    pthread_cond_t vazio;  // Condição para indicar que a fila está vazia e a thread de processamento deve esperar

} Fila; // Estrutura de dados para implementação da fila utilizada para comunicação entre as threads de leitura e processamento

typedef struct {

    double min_temp;
    double max_temp;
    double soma_temp;

    double min_umid;
    double max_umid;
    double soma_umid;

    double min_press;
    double max_press;
    double soma_press;

    char data_min_temp[50];
    char data_max_temp[50];

    char data_min_umid[50];
    char data_max_umid[50];

    char data_min_press[50];
    char data_max_press[50];

    double bateria_inicial;
    double bateria_final;
    char data_bateria_inicial[50];
    char data_bateria_final[50];

    int primeira_bateria;

    char sfs[10][20];
    int total_sf;

    long total; // Total de registros processados para a cidade
    int total_ids_sem_temp; // Contador para o número de registros sem dados de temperatura
    int total_ids_sem_umid; // Contador para o número de registros sem dados de umidade
    int total_ids_sem_press; // Contador para o número de registros sem dados

} Estatisticas; // Estrutura para armazenar as estatísticas calculadas para cada cidade, incluindo mínimos, máximos, médias, dados de bateria e spreading factors utilizados

Fila fila;

Estatisticas caxias;
Estatisticas bento;

pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER; // Mutex para controle de acesso ao arquivo de log, garantindo que as mensagens sejam registradas de forma consistente e sem conflitos entre as threads

long ids_lidos[50000]; // Array para armazenar os IDs já lidos, utilizado para evitar processar registros duplicados    
int total_ids = 0; // Contador para o número total de IDs armazenados no array ids_lidos

pthread_mutex_t ids_mutex = PTHREAD_MUTEX_INITIALIZER; // Mutex para controle de acesso ao array ids_lidos e ao contador total_ids, garantindo que as operações de verificação e adição de IDs sejam realizadas de forma segura entre as threads

int terminou = 0; // Flag para indicar que a thread de leitura terminou

void iniciarFila() {

    fila.inicio = 0;
    fila.fim = 0;
    fila.quantidade = 0;

    pthread_mutex_init(&fila.mutex, NULL); // Inicializa o mutex para controle de acesso à fila

    pthread_cond_init(&fila.cheio, NULL);  // Inicializa a condição para indicar que a fila está cheia
    pthread_cond_init(&fila.vazio, NULL);  // Inicializa a condição para indicar que a fila está vazia
}

void adicionarFila(Registro r) {

    pthread_mutex_lock(&fila.mutex);

    while (fila.quantidade == BUFFER_SIZE)
        pthread_cond_wait(&fila.cheio, &fila.mutex);

    fila.buffer[fila.fim] = r;

    fila.fim = (fila.fim + 1) % BUFFER_SIZE;

    fila.quantidade++;

    pthread_cond_signal(&fila.vazio); // Sinaliza que a fila não está mais vazia, permitindo que a thread de processamento continue removendo registros

    pthread_mutex_unlock(&fila.mutex);
}

int removerFila(Registro *r) {

    pthread_mutex_lock(&fila.mutex);

    while (fila.quantidade == 0 && !terminou)
        pthread_cond_wait(&fila.vazio, &fila.mutex);

    if (fila.quantidade == 0 && terminou) {

        pthread_mutex_unlock(&fila.mutex);

        return 0;
    }

    *r = fila.buffer[fila.inicio];

    fila.inicio = (fila.inicio + 1) % BUFFER_SIZE;

    fila.quantidade--;

    pthread_cond_signal(&fila.cheio); // Sinaliza que a fila não está mais cheia, permitindo que a thread de leitura continue adicionando registros

    pthread_mutex_unlock(&fila.mutex);

    return 1;
}

// Função para registrar mensagens no arquivo de log, garantindo que o acesso ao arquivo seja controlado por um mutex para evitar conflitos entre as threads
void logMensagem(const char *msg) {

    pthread_mutex_lock(&log_mutex);

    FILE *f = fopen("processamento.log", "a");

    if (f) {

        fprintf(f, "%s\n", msg);

        fclose(f);
    }

    pthread_mutex_unlock(&log_mutex);
}

void limparLog() {

    FILE *f = fopen("processamento.log", "w");

    if (f) {
        fclose(f);
    }
}

// Função para verificar se um ID já foi lido, utilizando um array para armazenar os IDs e um mutex para controlar o acesso a esse array, garantindo que a verificação e adição de IDs sejam realizadas de forma segura entre as threads
int idRepetido(long id) {

    for (int i = 0; i < total_ids; i++) {

        if (ids_lidos[i] == id) {

            char buffer[150];

            sprintf(buffer,
                    "DADO DUPLICADO ENCONTRADO - ID duplicado: %ld",
                    id); // Mensagem de log indicando que um dado duplicado foi encontrado, com o ID do registro duplicado

            logMensagem(buffer);

            return 1;
        }
    }

    return 0;
}

// Função para inicializar a estrutura de estatísticas, definindo os valores mínimos e máximos para temperatura, umidade e pressão, além de inicializar as variáveis relacionadas à bateria e aos spreading factors utilizados
void iniciarEstatisticas(Estatisticas *e) {

    e->min_temp = 100;
    e->max_temp = -100;

    e->min_umid = 101;
    e->max_umid = -1;

    e->min_press = 2000;
    e->max_press = -1;

    e->soma_temp = 0;
    e->soma_umid = 0;
    e->soma_press = 0;

    e->total = 0;

    e->primeira_bateria = 1;

    e->total_sf = 0;

    e->bateria_inicial = 0;
    e->bateria_final = 0;

    strcpy(e->data_bateria_inicial, "");
    strcpy(e->data_bateria_final, "");
}

// Função para verificar se um spreading factor já foi registrado nas estatísticas, utilizando um array para armazenar os spreading factors e comparando o novo spreading factor com os já registrados
int sfExiste(Estatisticas *e, char *sf) {

    for (int i = 0; i < e->total_sf; i++) {

        if (strcmp(e->sfs[i], sf) == 0)
            return 1;
    }

    return 0;
}

// Função para atualizar as estatísticas com base em um novo registro, verificando os valores de temperatura, umidade, pressão e bateria, e atualizando os mínimos, máximos, médias e dados relacionados à bateria e spreading factors utilizados
void atualizarEstatisticas(Estatisticas *e, Registro r) {

    if (r.temperatura != -1000) { // Verifica se a temperatura é válida antes de atualizar as estatísticas, utilizando um valor específico (-1000) para indicar que a temperatura não foi fornecida no registro

        if (r.temperatura < e->min_temp) {

            e->min_temp = r.temperatura;

            strcpy(e->data_min_temp, r.datahora);
        }

        if (r.temperatura > e->max_temp) {

            e->max_temp = r.temperatura;

            strcpy(e->data_max_temp, r.datahora);
        }

        e->soma_temp += r.temperatura;
    } else {

        char buffer[150];

        sprintf(buffer,
                "REGISTRO SEM TEMPERATURA - ID: %ld",
                r.id);

        logMensagem(buffer);

        e->total_ids_sem_temp++;
    }

    if (r.umidade != -1) { // Verifica se a umidade é válida antes de atualizar as estatísticas, utilizando um valor específico (-1) para indicar que a umidade não foi fornecida no registro

        if (r.umidade < e->min_umid) {

            e->min_umid = r.umidade;

            strcpy(e->data_min_umid, r.datahora);
        }

        if (r.umidade > e->max_umid) {

            e->max_umid = r.umidade;

            strcpy(e->data_max_umid, r.datahora);
        }

        e->soma_umid += r.umidade;
    } else {

        char buffer[150];
        sprintf(buffer,
                "REGISTRO SEM UMIDADE - ID: %ld",
                r.id);

        logMensagem(buffer);

        e->total_ids_sem_umid++;
    }

    if (r.pressao != -1) { // Verifica se a pressão é válida antes de atualizar as estatísticas, utilizando um valor específico (-1) para indicar que a pressão não foi fornecida no registro

        if (r.pressao < e->min_press) {

            e->min_press = r.pressao;

            strcpy(e->data_min_press, r.datahora);
        }

        if (r.pressao > e->max_press) {

            e->max_press = r.pressao;

            strcpy(e->data_max_press, r.datahora);
        }

        e->soma_press += r.pressao;
    } else {

        char buffer[150];

        sprintf(buffer,
                "REGISTRO SEM PRESSÃO - ID: %ld",
                r.id);

        logMensagem(buffer);

        e->total_ids_sem_press++;
    }

    if (r.bateria != -1) { // Verifica se a bateria é válida antes de atualizar as estatísticas, utilizando um valor específico (-1) para indicar que a bateria não foi fornecida no registro

        if (e->primeira_bateria || strcmp(r.datahora, e->data_bateria_inicial) < 0) {

            e->bateria_inicial = r.bateria;

            strcpy(e->data_bateria_inicial, r.datahora);

            e->primeira_bateria = 0;
        }

        if (strlen(e->data_bateria_final) == 0 || strcmp(r.datahora, e->data_bateria_final) > 0) {

            e->bateria_final = r.bateria;

            strcpy(e->data_bateria_final, r.datahora);
        }
    }

    if (strlen(r.sf) > 0 && !sfExiste(e, r.sf)) { // Verifica se o spreading factor é válido e ainda não foi registrado nas estatísticas, utilizando a função sfExiste para verificar se o spreading factor já foi registrado antes de adicioná-lo ao array de spreading factors utilizados

        strcpy(e->sfs[e->total_sf], r.sf);

        e->total_sf++;
    }

    e->total++;
}

// Função para processar o payload de um registro, extraindo os valores de temperatura, umidade, pressão, bateria, data/hora e spreading factor, e preenchendo a estrutura do registro com esses valores
void processarPayload(cJSON *json, Registro *r) {

    // Inicializa os valores do registro com indicadores de dados ausentes, utilizando valores específicos para temperatura (-1000), umidade (-1), pressão (-1) e bateria (-1) para indicar que esses dados não foram fornecidos no payload
    r->temperatura = -1000;
    r->umidade = -1;
    r->pressao = -1;
    r->bateria = -1;

    strcpy(r->sf, "");

    // Extrai o nome do dispositivo para determinar a cidade correspondente, utilizando a função strstr para verificar se o nome do dispositivo contém a string "Caxias" e atribuindo a cidade "Caxias do Sul" ou "Bento Gonçalves" com base nessa verificação
    cJSON *device = cJSON_GetObjectItem(json, "device_name");

    if (strstr(device->valuestring, "Caxias"))
        strcpy(r->cidade, "Caxias do Sul");
    else
        strcpy(r->cidade, "Bento Gonçalves");

    cJSON *dados = cJSON_GetObjectItem(json, "data");

    int tam = cJSON_GetArraySize(dados); // Obtém o tamanho do array "data" no payload, utilizando a função cJSON_GetArraySize para determinar quantos itens estão presentes no array e iterar sobre eles para extrair os valores correspondentes de temperatura, umidade, pressão, bateria, data/hora e spreading factor

    for (int i = 0; i < tam; i++) {

        cJSON *item = cJSON_GetArrayItem(dados, i); // Itera sobre os itens do array "data" no payload, utilizando a função cJSON_GetArrayItem para obter cada item e processá-lo individualmente

        cJSON *var = cJSON_GetObjectItem(item, "variable"); // Extrai o nome da variável do item do payload, utilizando a função cJSON_GetObjectItem para obter o valor da chave "variable" e verificando se o valor é válido antes de continuar o processamento do item

        if (!var)
            continue;

        char *variavel = var->valuestring;

        // Verifica o nome da variável e extrai os valores correspondentes de temperatura, umidade, pressão, bateria, data/hora e spreading factor, preenchendo a estrutura do registro com esses valores
        if (strcmp(variavel, "temperature") == 0) {

            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor)
                r->temperatura = valor->valuedouble;

            cJSON *tempo = cJSON_GetObjectItem(item, "time");

            if (tempo)
                strcpy(r->datahora, tempo->valuestring);
        }

        else if (strcmp(variavel, "humidity") == 0) {

            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor)
                r->umidade = valor->valuedouble;
        }

        else if (strcmp(variavel, "airpressure") == 0) {

            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor)
                r->pressao = valor->valuedouble;
        }

        else if (strcmp(variavel, "batterylevel") == 0) {

            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor)
                r->bateria = valor->valuedouble;
        }

        else if (strcmp(variavel, "lora_spreading_factor") == 0) {

            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor)
                sprintf(r->sf, "SF%d", valor->valueint);
        }
    }
}

// Função para a thread de leitura, responsável por ler os arquivos JSON, processar os registros e adicionar os registros processados à fila para que a thread de processamento possa calcular as estatísticas, utilizando a biblioteca cJSON para parsear os arquivos JSON e extrair os dados necessários para preencher a estrutura do registro e atualizar as estatísticas
void *threadLeitura(void *arg) {

    char *arquivo = (char *) arg;

    FILE *f = fopen(arquivo, "r");

    if (!f) {

        logMensagem("Erro ao abrir arquivo");

        pthread_exit(NULL);
    }

    logMensagem("Iniciando leitura");

    fseek(f, 0, SEEK_END);

    long tamanho = ftell(f);

    rewind(f);

    char *conteudo = malloc(tamanho + 1);

    fread(conteudo, 1, tamanho, f);

    conteudo[tamanho] = '\0';

    fclose(f);

    cJSON *json = cJSON_Parse(conteudo);

    if (!json) {

        free(conteudo);

        pthread_exit(NULL);
    }

    int total = cJSON_GetArraySize(json);

    for (int i = 0; i < total; i++) {

        cJSON *obj = cJSON_GetArrayItem(json, i);

        cJSON *id_json = cJSON_GetObjectItem(obj, "id");

        if (!id_json)
            id_json = cJSON_GetObjectItem(obj, "payload_id");

        if (!id_json)
            continue;

        long id = id_json->valuedouble;

        pthread_mutex_lock(&ids_mutex);

        if (idRepetido(id)) {

            pthread_mutex_unlock(&ids_mutex);

            continue; 
        }

        ids_lidos[total_ids] = id;
        total_ids++;

        pthread_mutex_unlock(&ids_mutex);

        cJSON *payload = cJSON_GetObjectItem(obj, "payload");

        if (!payload)
            payload = cJSON_GetObjectItem(obj, "brute_data");

        if (!payload)
            continue;

        cJSON *payload_json = cJSON_Parse(payload->valuestring);

        if (!payload_json)
            continue;

        Registro r;

        memset(&r, 0, sizeof(Registro));

        r.id = id;

        processarPayload(payload_json, &r);

        adicionarFila(r);

        cJSON_Delete(payload_json);
    }

    cJSON_Delete(json);

    free(conteudo);

    logMensagem("Leitura finalizada");

    pthread_exit(NULL);
}

void *threadProcessamento(void *arg) {

    Registro r;

    while (removerFila(&r)) {

        if (strcmp(r.cidade, "Caxias do Sul") == 0)
            atualizarEstatisticas(&caxias, r);

        else
            atualizarEstatisticas(&bento, r);
    }

    logMensagem("Processamento finalizado");

    pthread_exit(NULL);
}

// Função para a thread de logger, responsável por registrar mensagens de log periodicamente enquanto o sistema está executando, utilizando um loop que verifica a flag de término e registra mensagens a cada 2 segundos, e registrando uma mensagem final quando o logger é finalizado
void *threadLogger(void *arg) {

    while (!terminou) {

        logMensagem("Sistema executando");

        sleep(2);
    }

    logMensagem("Logger finalizado");

    pthread_exit(NULL);
}

// Função para imprimir os spreading factors utilizados nas estatísticas, iterando sobre o array de spreading factors e imprimindo cada um
void imprimirSF(Estatisticas *e) {

    for (int i = 0; i < e->total_sf; i++) {

        printf("%s", e->sfs[i]);

        if (i < e->total_sf - 1)
            printf(", ");
    }
}

int main() {

    limparLog();

    struct timespec inicio, fim;

    clock_gettime(CLOCK_MONOTONIC, &inicio);

    iniciarFila();

    iniciarEstatisticas(&caxias);
    iniciarEstatisticas(&bento);

    pthread_t leitura1;
    pthread_t leitura2;
    pthread_t processamento;
    pthread_t logger;

    pthread_create(&leitura1, NULL,
                   threadLeitura,
                   "mqtt_senzemo_cx_bg.json");

    pthread_create(&leitura2, NULL,
                   threadLeitura,
                   "senzemo_cx_bg.json");

    pthread_create(&processamento, NULL,
                   threadProcessamento,
                   NULL);

    pthread_create(&logger, NULL,
                   threadLogger,
                   NULL);

    pthread_join(leitura1, NULL);
    pthread_join(leitura2, NULL);

    terminou = 1;

    pthread_cond_broadcast(&fila.vazio);

    pthread_join(processamento, NULL);
    pthread_join(logger, NULL);

    clock_gettime(CLOCK_MONOTONIC, &fim);

    double tempo =
        (fim.tv_sec - inicio.tv_sec) +
        (fim.tv_nsec - inicio.tv_nsec) / 1e9;

    printf("============================================================\n");
    printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n\n");

    printf("------------------------------------------------------------\n");
    printf("TEMPERATURA (°C)\n");
    printf("------------------------------------------------------------\n");

    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printf("-----------------------------------------------------------------------------------------------\n");

    printf("Caxias do Sul     | %.2f | %s | %.2f | %s | %.2f\n",
           caxias.min_temp,
           caxias.data_min_temp,
           caxias.max_temp,
           caxias.data_max_temp,
           caxias.soma_temp / (caxias.total - caxias.total_ids_sem_temp));

    printf("Bento Gonçalves   | %.2f | %s | %.2f | %s | %.2f\n",
           bento.min_temp,
           bento.data_min_temp,
           bento.max_temp,
           bento.data_max_temp,
           bento.soma_temp / (bento.total - bento.total_ids_sem_temp));

    printf("\n------------------------------------------------------------\n");
    printf("UMIDADE (%%)\n");
    printf("------------------------------------------------------------\n");

    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printf("-----------------------------------------------------------------------------------------------\n");

    printf("Caxias do Sul     | %.2f | %s | %.2f | %s | %.2f\n",
           caxias.min_umid,
           caxias.data_min_umid,
           caxias.max_umid,
           caxias.data_max_umid,
           caxias.soma_umid / (caxias.total - caxias.total_ids_sem_umid));

    printf("Bento Gonçalves   | %.2f | %s | %.2f | %s | %.2f\n",
           bento.min_umid,
           bento.data_min_umid,
           bento.max_umid,
           bento.data_max_umid,
           bento.soma_umid / (bento.total - bento.total_ids_sem_umid));

    printf("\n------------------------------------------------------------\n");
    printf("PRESSÃO ATMOSFÉRICA (hPa)\n");
    printf("------------------------------------------------------------\n");

    printf("Cidade            | Mínima | Data/Hora             | Máxima | Data/Hora             | Média\n");
    printf("-----------------------------------------------------------------------------------------------\n");

    printf("Caxias do Sul     | %.2f | %s | %.2f | %s | %.2f\n",
           caxias.min_press,
           caxias.data_min_press,
           caxias.max_press,
           caxias.data_max_press,
           caxias.soma_press / (caxias.total - caxias.total_ids_sem_press));

    printf("Bento Gonçalves   | %.2f | %s | %.2f | %s | %.2f\n",
           bento.min_press,
           bento.data_min_press,
           bento.max_press,
           bento.data_max_press,
           bento.soma_press / (bento.total - bento.total_ids_sem_press));

    printf("\n------------------------------------------------------------\n");
    printf("BATERIA\n");
    printf("------------------------------------------------------------\n");

    printf("Cidade            | Inicial (V) | Final (V) | Consumo (V)\n");
    printf("------------------------------------------------------------\n");

    printf("Caxias do Sul     | %.2f | %.2f | %.2f\n",
           caxias.bateria_inicial,
           caxias.bateria_final,
           caxias.bateria_inicial - caxias.bateria_final);

    printf("Bento Gonçalves   | %.2f | %.2f | %.2f\n",
           bento.bateria_inicial,
           bento.bateria_final,
           bento.bateria_inicial - bento.bateria_final);

    printf("\n------------------------------------------------------------\n");
    printf("SPREADING FACTORS UTILIZADOS\n");
    printf("------------------------------------------------------------\n");

    printf("Cidade            | SF utilizados\n");
    printf("------------------------------------------------------------\n");

    printf("Caxias do Sul     | ");
    imprimirSF(&caxias);

    printf("\n");

    printf("Bento Gonçalves   | ");
    imprimirSF(&bento);

    printf("\n");

    printf("\n------------------------------------------------------------\n");
    printf("DESEMPENHO\n");
    printf("------------------------------------------------------------\n");

    printf("Tempo total de execução: %.2f segundos\n", tempo);

    printf("Threads utilizadas: 4\n");

    printf(" - Thread 1: leitura dos dados do primeiro arquivo\n");
    printf(" - Thread 2: leitura dos dados do segundo arquivo\n");
    printf(" - Thread 3: cálculo das estatísticas\n");
    printf(" - Thread 4: registro de logs\n");

    printf("\nArquivo de log gerado: processamento.log\n");

    printf("\n============================================================\n");
    printf("Processamento finalizado com sucesso.\n");
    printf("============================================================\n");

    return 0;
}