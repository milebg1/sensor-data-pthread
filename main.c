#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "cJSON.h"

#define BUFFER_SIZE 50000
#define HASH_SIZE 50000
#define LOG_QUEUE_SIZE 1024
#define LOG_MSG_SIZE 256

// Nó para a tabela hash de detecção de duplicatas.
typedef struct StringHashNode {
    char *chave;
    struct StringHashNode *prox;
} StringHashNode;

// Registro normalizado depois da leitura do JSON.
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

// Fila de registros entre leitura e processamento.
typedef struct {
    Registro buffer[BUFFER_SIZE];

    int inicio;
    int fim;
    int quantidade;

    pthread_mutex_t mutex;
    pthread_cond_t cheio;
    pthread_cond_t vazio;
} Fila;

// Estatisticas acumuladas por cidade.
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

    long total;
    int total_ids_sem_temp;
    int total_ids_sem_umid;
    int total_ids_sem_press;
} Estatisticas;

// Fila de mensagens consumida apenas pela thread de log.
typedef struct {
    char mensagens[LOG_QUEUE_SIZE][LOG_MSG_SIZE];

    int inicio;
    int fim;
    int quantidade;
    int ativo;

    pthread_mutex_t mutex;
    pthread_cond_t nao_vazia;
    pthread_cond_t nao_cheia;
} FilaLog;

Fila fila;
FilaLog fila_log;

Estatisticas caxias;
Estatisticas bento;

StringHashNode *duplicatas_hash[HASH_SIZE];
pthread_mutex_t duplicatas_mutex[HASH_SIZE];

int terminou = 0;

/**********************************************************/
/******************** Fila de Registros *******************/
/**********************************************************/

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

    while (fila.quantidade == BUFFER_SIZE) {
        pthread_cond_wait(&fila.cheio, &fila.mutex);
    }

    fila.buffer[fila.fim] = r;
    fila.fim = (fila.fim + 1) % BUFFER_SIZE;
    fila.quantidade++;

    pthread_cond_signal(&fila.vazio);
    pthread_mutex_unlock(&fila.mutex);
}

int removerFila(Registro *r) {
    pthread_mutex_lock(&fila.mutex);

    while (fila.quantidade == 0 && !terminou) {
        pthread_cond_wait(&fila.vazio, &fila.mutex);
    }

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

/**********************************************************/
/*********************** Fila de Logs *********************/
/**********************************************************/

void iniciarFilaLog() {
    fila_log.inicio = 0;
    fila_log.fim = 0;
    fila_log.quantidade = 0;
    fila_log.ativo = 1;

    pthread_mutex_init(&fila_log.mutex, NULL);
    pthread_cond_init(&fila_log.nao_vazia, NULL);
    pthread_cond_init(&fila_log.nao_cheia, NULL);
}

// Apenas coloca a mensagem na fila; a thread de log escreve no arquivo.
void logMensagem(const char *msg) {
    pthread_mutex_lock(&fila_log.mutex);

    while (fila_log.quantidade == LOG_QUEUE_SIZE && fila_log.ativo) {
        pthread_cond_wait(&fila_log.nao_cheia, &fila_log.mutex);
    }

    if (!fila_log.ativo) {
        pthread_mutex_unlock(&fila_log.mutex);
        return;
    }
    strncpy(fila_log.mensagens[fila_log.fim], msg, LOG_MSG_SIZE - 1);
    fila_log.mensagens[fila_log.fim][LOG_MSG_SIZE - 1] = '\0';
    fila_log.fim = (fila_log.fim + 1) % LOG_QUEUE_SIZE;
    fila_log.quantidade++;

    pthread_cond_signal(&fila_log.nao_vazia); // Acorda a thread de log para processar a nova mensagem.
    pthread_mutex_unlock(&fila_log.mutex);
}

// Sinaliza para a thread de log que não haverá mais mensagens e acorda ela caso esteja esperando.
void pararFilaLog() {
    pthread_mutex_lock(&fila_log.mutex);

    fila_log.ativo = 0;

    pthread_cond_signal(&fila_log.nao_vazia);
    pthread_cond_broadcast(&fila_log.nao_cheia);
    pthread_mutex_unlock(&fila_log.mutex);
}

// Destrói os mutexes e condições da fila de log.
void destruirFilaLog() {
    pthread_mutex_destroy(&fila_log.mutex);
    pthread_cond_destroy(&fila_log.nao_vazia);
    pthread_cond_destroy(&fila_log.nao_cheia);
}

void *threadLogger(void *arg) {
    (void)arg;

    FILE *f = fopen("processamento.log", "a");

    while (1) {
        pthread_mutex_lock(&fila_log.mutex);

        while (fila_log.quantidade == 0 && fila_log.ativo) {
            pthread_cond_wait(&fila_log.nao_vazia, &fila_log.mutex);
        }

        if (fila_log.quantidade == 0 && !fila_log.ativo) {
            pthread_mutex_unlock(&fila_log.mutex);
            break;
        }
        char msg[LOG_MSG_SIZE];
        strcpy(msg, fila_log.mensagens[fila_log.inicio]);
        fila_log.inicio = (fila_log.inicio + 1) % LOG_QUEUE_SIZE;
        fila_log.quantidade--;
        pthread_cond_signal(&fila_log.nao_cheia);
        pthread_mutex_unlock(&fila_log.mutex);

        if (f) {
            fprintf(f, "%s\n", msg);
            fflush(f);
        }
    }

    if (f) {
        fclose(f);
    }

    return NULL;
}

// Limpa o arquivo de log no início do programa para evitar acumular logs de execuções anteriores.
void limparLog() {
    FILE *f = fopen("processamento.log", "w");

    if (f) {
        fclose(f);
    }
}

/**********************************************************/
/*********** Hash para Verificacao de Duplicatas **********/
/**********************************************************/

// Função de hash simples para strings, usada para detectar payloads duplicados. O resultado é o índice na tabela hash.
unsigned int hashString(const char *str) {
    unsigned int valor = 5381;

    while (*str) {
        valor = ((valor << 5) + valor) + (unsigned char) *str;
        str++;
    }

    return valor % HASH_SIZE;
}

// Inicializa a tabela hash e os mutexes para controle de acesso.
void iniciarHashDuplicatas() {
    for (int i = 0; i < HASH_SIZE; i++) {
        duplicatas_hash[i] = NULL;
        pthread_mutex_init(&duplicatas_mutex[i], NULL);
    }
}

// Obtém a chave para verificação de duplicidade. Para o arquivo "senzemo_cx_bg.json", a chave é limitada até o campo "snr" para evitar diferenças irrelevantes.
const char *obterChaveDuplicata(const char *arquivo,
                                const char *payload,
                                char *chave_limitada,
                                size_t tamanho_chave_limitada) {
    const char *chave = payload;

    if (payload &&
        strcmp(arquivo, "senzemo_cx_bg.json") == 0) {
        const char *snr_pos = strstr(payload, "\"snr\"");

        if (snr_pos) {
            const char *fim = strchr(snr_pos, ',');

            if (!fim) {
                fim = payload + strlen(payload);
            }

            size_t tamanho = fim - payload;

            if (tamanho >= tamanho_chave_limitada) {
                tamanho = tamanho_chave_limitada - 1;
            }

            strncpy(chave_limitada, payload, tamanho);
            chave_limitada[tamanho] = '\0';
            chave = chave_limitada;
        }
    }
    return chave;
}

// Registra o payload se ele ainda nao apareceu.
int registrarPayloadSeNovo(const char *chave, long id) {
    if (!chave) {
        return 0;
    }

    unsigned int indice = hashString(chave);

    pthread_mutex_lock(&duplicatas_mutex[indice]);

    StringHashNode *atual = duplicatas_hash[indice];

    while (atual) {
        if (strcmp(atual->chave, chave) == 0) {
            pthread_mutex_unlock(&duplicatas_mutex[indice]);

            char buffer[150];

            sprintf(buffer,
                    "DADO DUPLICADO ENCONTRADO - payload duplicado: ID %ld",
                    id);
            logMensagem(buffer);

            return 0;
        }

        atual = atual->prox;
    }

    StringHashNode *novo = malloc(sizeof(StringHashNode));

    if (!novo) {
        pthread_mutex_unlock(&duplicatas_mutex[indice]);
        logMensagem("Erro ao alocar memoria para armazenar payload na tabela hash");

        return 0;
    }

    novo->chave = malloc(strlen(chave) + 1);

    if (!novo->chave) {
        free(novo);
        pthread_mutex_unlock(&duplicatas_mutex[indice]);
        logMensagem("Erro ao alocar memoria para armazenar chave de payload");

        return 0;
    }

    strcpy(novo->chave, chave);
    novo->prox = duplicatas_hash[indice];
    duplicatas_hash[indice] = novo;

    pthread_mutex_unlock(&duplicatas_mutex[indice]);

    return 1;
}

// Libera a memória alocada para a tabela hash de duplicatas e destrói os mutexes.
void liberarHashDuplicatas() {
    for (int i = 0; i < HASH_SIZE; i++) {
        pthread_mutex_lock(&duplicatas_mutex[i]);

        StringHashNode *atual = duplicatas_hash[i];

        while (atual) {
            StringHashNode *proximo = atual->prox;

            free(atual->chave);
            free(atual);
            atual = proximo;
        }

        duplicatas_hash[i] = NULL;

        pthread_mutex_unlock(&duplicatas_mutex[i]);
        pthread_mutex_destroy(&duplicatas_mutex[i]);
    }
}

/**********************************************************/
/******************* Processamento de Dados ***************/
/**********************************************************/

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

// Verifica se o spreading factor já foi registrado para a cidade.
int sfExiste(Estatisticas *e, char *sf) {
    for (int i = 0; i < e->total_sf; i++) {
        if (strcmp(e->sfs[i], sf) == 0) {
            return 1;
        }
    }
    return 0;
}

// Atualiza as estatísticas com base no registro processado, verificando cada campo e atualizando mínimos, máximos, somas e contadores de dados faltantes.
void atualizarEstatisticas(Estatisticas *e, Registro r) {
    if (r.temperatura != -1000) {
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
    } else {
        char buffer[150];

        sprintf(buffer,
                "REGISTRO SEM UMIDADE - ID: %ld",
                r.id);

        logMensagem(buffer);
        e->total_ids_sem_umid++;
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
    } else {
        char buffer[150];

        sprintf(buffer,
                "REGISTRO SEM PRESSÃO - ID: %ld",
                r.id);

        logMensagem(buffer);
        e->total_ids_sem_press++;
    }

    if (r.bateria != -1) {
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

    if (strlen(r.sf) > 0 && !sfExiste(e, r.sf)) {
        strcpy(e->sfs[e->total_sf], r.sf);
        e->total_sf++;
    }

    e->total++;
}

// Extrai os campos importantes do payload.
void processarPayload(cJSON *json, Registro *r) {
    r->temperatura = -1000;
    r->umidade = -1;
    r->pressao = -1;
    r->bateria = -1;
    strcpy(r->sf, "");

    cJSON *device = cJSON_GetObjectItem(json, "device_name");

    if (strstr(device->valuestring, "Caxias")) {
        strcpy(r->cidade, "Caxias do Sul");
    } else {
        strcpy(r->cidade, "Bento Gonçalves");
    }

    cJSON *dados = cJSON_GetObjectItem(json, "data");
    int tam = cJSON_GetArraySize(dados);

    for (int i = 0; i < tam; i++) {
        cJSON *item = cJSON_GetArrayItem(dados, i);
        cJSON *var = cJSON_GetObjectItem(item, "variable");

        if (!var) {
            continue;
        }

        char *variavel = var->valuestring;

        if (strcmp(variavel, "temperature") == 0) {
            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor) {
                r->temperatura = valor->valuedouble;
            }

            cJSON *tempo = cJSON_GetObjectItem(item, "time");

            if (tempo) {
                strcpy(r->datahora, tempo->valuestring);
            }
        } else if (strcmp(variavel, "humidity") == 0) {
            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor) {
                r->umidade = valor->valuedouble;
            }
        } else if (strcmp(variavel, "airpressure") == 0) {
            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor) {
                r->pressao = valor->valuedouble;
            }
        } else if (strcmp(variavel, "batterylevel") == 0) {
            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor) {
                r->bateria = valor->valuedouble;
            }
        } else if (strcmp(variavel, "lora_spreading_factor") == 0) {
            cJSON *valor = cJSON_GetObjectItem(item, "value");

            if (valor) {
                sprintf(r->sf, "SF%d", valor->valueint);
            }
        }
    }
}

/**********************************************************/
/********************* Leitura dos Dados ******************/
/**********************************************************/

void *threadLeitura(void *arg) {
    char *arquivo = (char *) arg;
    char buffer_log[150];

    FILE *f = fopen(arquivo, "r");

    if (!f) {
        snprintf(buffer_log, sizeof(buffer_log), "Erro ao abrir arquivo: %s", arquivo);
        logMensagem(buffer_log);
        return NULL;
    }

    snprintf(buffer_log, sizeof(buffer_log), "Iniciando leitura: %s", arquivo);
    logMensagem(buffer_log);

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
        return NULL;
    }

    int total = cJSON_GetArraySize(json);

    for (int i = 0; i < total; i++) {
        cJSON *obj = cJSON_GetArrayItem(json, i);
        cJSON *id_json = cJSON_GetObjectItem(obj, "id");

        if (!id_json) {
            id_json = cJSON_GetObjectItem(obj, "payload_id");
        }

        if (!id_json) {
            continue;
        }

        long id = id_json->valuedouble;

        cJSON *payload = cJSON_GetObjectItem(obj, "payload");

        if (!payload) {
            payload = cJSON_GetObjectItem(obj, "brute_data");
        }

        if (!payload) {
            continue;
        }

        char chave_limitada[5000];
        const char *chave_duplicata = obterChaveDuplicata(arquivo,
                                                          payload->valuestring,
                                                          chave_limitada,
                                                          sizeof(chave_limitada));

        if (!registrarPayloadSeNovo(chave_duplicata, id)) {
            continue;
        }

        cJSON *payload_json = cJSON_Parse(payload->valuestring);

        if (!payload_json) {
            continue;
        }

        Registro r;

        memset(&r, 0, sizeof(Registro));
        r.id = id;

        processarPayload(payload_json, &r);
        adicionarFila(r);

        cJSON_Delete(payload_json);
    }

    cJSON_Delete(json);
    free(conteudo);

    snprintf(buffer_log, sizeof(buffer_log), "Leitura finalizada: %s", arquivo);
    logMensagem(buffer_log);

    return NULL;
}

/**********************************************************/
/******************* Threads de Trabalho ******************/
/**********************************************************/

void *threadProcessamento(void *arg) {
    (void)arg;

    Registro r;

    while (removerFila(&r)) {
        if (strcmp(r.cidade, "Caxias do Sul") == 0) {
            atualizarEstatisticas(&caxias, r);
        } else {
            atualizarEstatisticas(&bento, r);
        }
    }
    logMensagem("Processamento finalizado");

    return NULL;
}

/**********************************************************/
/******************** Saida de Resultados *****************/
/**********************************************************/

// Imprime os spreading factors utilizados para cada cidade, separados por vírgula.
void imprimirSF(Estatisticas *e) {
    for (int i = 0; i < e->total_sf; i++) {
        printf("%s", e->sfs[i]);

        if (i < e->total_sf - 1) {
            printf(", ");
        }
    }
}

/**********************************************************/
/************************** Main **************************/
/**********************************************************/

int main() {
    limparLog();
    iniciarFilaLog();

    struct timespec inicio, fim;

    clock_gettime(CLOCK_MONOTONIC, &inicio);

    iniciarFila();
    iniciarHashDuplicatas();

    iniciarEstatisticas(&caxias);
    iniciarEstatisticas(&bento);

    pthread_t leitura1;
    pthread_t leitura2;
    pthread_t processamento;
    pthread_t logger;

    pthread_create(&logger, NULL,
                   threadLogger,
                   NULL);

    pthread_create(&leitura1, NULL,
                   threadLeitura,
                   "mqtt_senzemo_cx_bg.json");

    pthread_create(&leitura2, NULL,
                   threadLeitura,
                   "senzemo_cx_bg.json");

    pthread_create(&processamento, NULL,
                   threadProcessamento,
                   NULL);

    pthread_join(leitura1, NULL);
    pthread_join(leitura2, NULL);

    terminou = 1;
    pthread_cond_broadcast(&fila.vazio);

    pthread_join(processamento, NULL);

    pararFilaLog();
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

    liberarHashDuplicatas();
    destruirFilaLog();

    return 0;
}
