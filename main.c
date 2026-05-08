#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <float.h>
#include <unistd.h>
#include "cJSON.h"

#define BUFFER_SIZE 1000

typedef struct {

    char cidade[50];

    double temperatura;
    double umidade;
    double pressao;
    double bateria;

    char datahora[50];
    char sf[20];

    long id;

} Registro;

typedef struct {

    Registro buffer[BUFFER_SIZE];

    int inicio;
    int fim;
    int quantidade;

    pthread_mutex_t mutex;

    pthread_cond_t cheio;
    pthread_cond_t vazio;

} Fila;

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

    int primeira_bateria;

    char sfs[10][20];
    int total_sf;

    long total;

} Estatisticas;

Fila fila;

Estatisticas caxias;
Estatisticas bento;

pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

long ids_lidos[50000];
int total_ids = 0;

pthread_mutex_t ids_mutex = PTHREAD_MUTEX_INITIALIZER;

int terminou = 0;

void iniciarFila() {

    fila.inicio = 0;
    fila.fim = 0;
    fila.quantidade = 0;

    pthread_mutex_init(&fila.mutex, NULL);

    pthread_cond_init(&fila.cheio, NULL);
    pthread_cond_init(&fila.vazio, NULL);
}

void adicionarFila(Registro r) {

    pthread_mutex_lock(&fila.mutex);

    while (fila.quantidade == BUFFER_SIZE)
        pthread_cond_wait(&fila.cheio, &fila.mutex);

    fila.buffer[fila.fim] = r;

    fila.fim = (fila.fim + 1) % BUFFER_SIZE;

    fila.quantidade++;

    pthread_cond_signal(&fila.vazio);

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

    pthread_cond_signal(&fila.cheio);

    pthread_mutex_unlock(&fila.mutex);

    return 1;
}

void logMensagem(const char *msg) {

    pthread_mutex_lock(&log_mutex);

    FILE *f = fopen("processamento.log", "a");

    if (f) {

        fprintf(f, "%s\n", msg);

        fclose(f);
    }

    pthread_mutex_unlock(&log_mutex);
}

int idRepetido(long id) {

    for (int i = 0; i < total_ids; i++) {

        if (ids_lidos[i] == id)
            return 1;
    }

    return 0;
}

void iniciarEstatisticas(Estatisticas *e) {

    e->min_temp = DBL_MAX;
    e->max_temp = -DBL_MAX;

    e->min_umid = DBL_MAX;
    e->max_umid = -DBL_MAX;

    e->min_press = DBL_MAX;
    e->max_press = -DBL_MAX;

    e->soma_temp = 0;
    e->soma_umid = 0;
    e->soma_press = 0;

    e->total = 0;

    e->primeira_bateria = 1;

    e->total_sf = 0;
}

int sfExiste(Estatisticas *e, char *sf) {

    for (int i = 0; i < e->total_sf; i++) {

        if (strcmp(e->sfs[i], sf) == 0)
            return 1;
    }

    return 0;
}

void atualizarEstatisticas(Estatisticas *e, Registro r) {

    if (r.temperatura != -1) {

        if (r.temperatura < e->min_temp) {

            e->min_temp = r.temperatura;

            strcpy(e->data_min_temp, r.datahora);
        }

        if (r.temperatura > e->max_temp) {

            e->max_temp = r.temperatura;

            strcpy(e->data_max_temp, r.datahora);
        }

        e->soma_temp += r.temperatura;
    }

    if (r.umidade != -1) {

        if (r.umidade < e->min_umid) {

            e->min_umid = r.umidade;

            strcpy(e->data_min_umid, r.datahora);
        }

        if (r.umidade > e->max_umid) {

            e->max_umid = r.umidade;

            strcpy(e->data_max_umid, r.datahora);
        }

        e->soma_umid += r.umidade;
    }

    if (r.pressao != -1) {

        if (r.pressao < e->min_press) {

            e->min_press = r.pressao;

            strcpy(e->data_min_press, r.datahora);
        }

        if (r.pressao > e->max_press) {

            e->max_press = r.pressao;

            strcpy(e->data_max_press, r.datahora);
        }

        e->soma_press += r.pressao;
    }

    if (r.bateria != -1) {

        if (e->primeira_bateria) {

            e->bateria_inicial = r.bateria;

            e->primeira_bateria = 0;
        }

        e->bateria_final = r.bateria;
    }

    if (strlen(r.sf) > 0) {

        if (!sfExiste(e, r.sf)) {

            strcpy(e->sfs[e->total_sf], r.sf);

            e->total_sf++;
        }
    }

    e->total++;
}

void processarPayload(cJSON *json, Registro *r) {

    r->temperatura = -1;
    r->umidade = -1;
    r->pressao = -1;
    r->bateria = -1;

    strcpy(r->sf, "");

    cJSON *device = cJSON_GetObjectItem(json, "device_name");

    if (strstr(device->valuestring, "Caxias"))
        strcpy(r->cidade, "Caxias do Sul");
    else
        strcpy(r->cidade, "Bento Gonçalves");

    cJSON *dados = cJSON_GetObjectItem(json, "data");

    int tam = cJSON_GetArraySize(dados);

    for (int i = 0; i < tam; i++) {

        cJSON *item = cJSON_GetArrayItem(dados, i);

        cJSON *var = cJSON_GetObjectItem(item, "variable");

        if (!var)
            continue;

        char *variavel = var->valuestring;

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

        else if (strcmp(variavel, "spreading_factor") == 0) {

            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor)
                sprintf(r->sf, "SF%d", valor->valueint);
        }
    }
}

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

        cJSON *payload_json =
            cJSON_Parse(payload->valuestring);

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

void *threadLogger(void *arg) {

    while (!terminou) {

        logMensagem("Sistema executando");

        sleep(2);
    }

    logMensagem("Logger finalizado");

    pthread_exit(NULL);
}

void imprimirSF(Estatisticas *e) {

    for (int i = 0; i < e->total_sf; i++) {

        printf("%s", e->sfs[i]);

        if (i < e->total_sf - 1)
            printf(", ");
    }
}

int main() {

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
           caxias.soma_temp / caxias.total);

    printf("Bento Gonçalves   | %.2f | %s | %.2f | %s | %.2f\n",
           bento.min_temp,
           bento.data_min_temp,
           bento.max_temp,
           bento.data_max_temp,
           bento.soma_temp / bento.total);

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
           caxias.soma_umid / caxias.total);

    printf("Bento Gonçalves   | %.2f | %s | %.2f | %s | %.2f\n",
           bento.min_umid,
           bento.data_min_umid,
           bento.max_umid,
           bento.data_max_umid,
           bento.soma_umid / bento.total);

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
           caxias.soma_press / caxias.total);

    printf("Bento Gonçalves   | %.2f | %s | %.2f | %s | %.2f\n",
           bento.min_press,
           bento.data_min_press,
           bento.max_press,
           bento.data_max_press,
           bento.soma_press / bento.total);

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

    printf("Threads utilizadas: 3\n");

    printf(" - Thread 1: leitura dos dados\n");
    printf(" - Thread 2: cálculo das estatísticas\n");
    printf(" - Thread 3: registro de logs\n");

    printf("\nArquivo de log gerado: processamento.log\n");

    printf("\n============================================================\n");
    printf("Processamento finalizado com sucesso.\n");
    printf("============================================================\n");

    return 0;
}