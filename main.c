#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <float.h>
#include <time.h>
#include "cJSON.h"

#define MAX_TEXTO 128
#define CAP_INICIAL 2000

//estruturas
typedef struct{
    char timestamp[MAX_TEXTO];
    double valor;
} Medicao;

typedef struct{
    Medicao *itens;
    size_t total;
    size_t capacidade;
} ListaMedicoes;

typedef struct{
    char nome_cidade[MAX_TEXTO];
    ListaMedicoes temp, umid, pressao, bateria;
    int sf_utilizados[13]; 
    size_t registros_processados;
} DadosCidade;

typedef struct{
    DadosCidade *caxias;
    DadosCidade *bento;
    const char *arquivo;
} ThreadArgs;

//funçoes de apoio
void inicializar_lista(ListaMedicoes *l){
    l->itens = malloc(CAP_INICIAL * sizeof(Medicao));
    l->total = 0;
    l->capacidade = CAP_INICIAL;
}

void inicializar_cidade(DadosCidade *c, const char *nome){
    memset(c, 0, sizeof(*c));
    strncpy(c->nome_cidade, nome, MAX_TEXTO - 1);
    inicializar_lista(&c->temp);
    inicializar_lista(&c->umid);
    inicializar_lista(&c->pressao);
    inicializar_lista(&c->bateria);
}

void inserir_medicao(ListaMedicoes *l, const char *t, double v) {
    //Eliminação de duplicatas simples
    if (l->total > 0 && strcmp(l->itens[l->total-1].timestamp, t) == 0 && l->itens[l->total-1].valor == v) return;

    if (l->total >= l->capacidade){
        l->capacidade *= 2;
        l->itens = realloc(l->itens, l->capacidade * sizeof(Medicao));
    }
    strncpy(l->itens[l->total].timestamp, t, MAX_TEXTO - 1);
    l->itens[l->total].valor = v;
    l->total++;
}

// ---Threads

// Thread 1: Leitura e extração
void* thread_leitura(void* arg) {
    ThreadArgs *a = (ThreadArgs*)arg;
    FILE *f = fopen(a->arquivo, "rb");
    if (!f) return NULL;

    fseek(f, 0, SEEK_END);
    long tam = ftell(f);
    rewind(f);
    char *buf = malloc(tam + 1);
    fread(buf, 1, tam, f);
    buf[tam] = '\0';
    fclose(f);

    cJSON *root = cJSON_Parse(buf);
    if (!root) { free(buf); return NULL; }

    cJSON *item;
    cJSON_ArrayForEach(item, root) {
        cJSON *payload_raw = cJSON_GetObjectItem(item, "payload");
        if (!payload_raw) payload_raw = cJSON_GetObjectItem(item, "brute_data");
        if (!payload_raw || !payload_raw->valuestring) continue;

        cJSON *inner = cJSON_Parse(payload_raw->valuestring);
        if (!inner) continue;

        cJSON *dev = cJSON_GetObjectItem(inner, "device_name");
        if (!dev || !dev->valuestring) { cJSON_Delete(inner); continue; }

        DadosCidade *alvo = NULL;
        if (strcasestr(dev->valuestring, "Caxias")) alvo = a->caxias;
        else if (strcasestr(dev->valuestring, "Bento")) alvo = a->bento;

        if (alvo) {
            alvo->registros_processados++;
            cJSON *data = cJSON_GetObjectItem(inner, "data");
            cJSON *d;
            cJSON_ArrayForEach(d, data) {
                cJSON *v_obj = cJSON_GetObjectItem(d, "variable");
                cJSON *t_obj = cJSON_GetObjectItem(d, "time");
                cJSON *val_obj = cJSON_GetObjectItem(d, "value");

                if (!v_obj || !t_obj || !val_obj) continue;

                const char *v = v_obj->valuestring;
                const char *t = t_obj->valuestring;
                double val = val_obj->valuedouble;

                if (strstr(v, "temperature")) inserir_medicao(&alvo->temp, t, val);
                else if (strstr(v, "humidity")) inserir_medicao(&alvo->umid, t, val);
                else if (strstr(v, "pressure")) inserir_medicao(&alvo->pressao, t, val);
                else if (strstr(v, "battery")) inserir_medicao(&alvo->bateria, t, val);
                else if (strstr(v, "spreading_factor")) {
                    int sf = (int)val;
                    if (sf >= 7 && sf <= 12) alvo->sf_utilizados[sf] = 1;
                }
            }
        }
        cJSON_Delete(inner);
    }
    cJSON_Delete(root);
    free(buf);
    return NULL;
}

// Thread 3: Log
void* thread_log(void* arg) {
    FILE *log = fopen("processamento.log", "a");
    if (log) {
        fprintf(log, "Processamento realizado em: %ld\n", time(NULL));
        fclose(log);
    }
    return NULL;
}

// --- Relatórios ---
void mostrar_estatisticas(DadosCidade *c, ListaMedicoes *l) {
    if (l->total == 0) return;
    double min = DBL_MAX, max = -DBL_MAX, soma = 0;
    int imin = 0, imax = 0;

    for (size_t i = 0; i < l->total; i++) {
        if (l->itens[i].valor < min) { min = l->itens[i].valor; imin = i; }
        if (l->itens[i].valor > max) { max = l->itens[i].valor; imax = i; }
        soma += l->itens[i].valor;
    }
    printf("%-18s | %6.2f | %-19.19s | %6.2f | %-19.19s | %6.2f\n", 
           c->nome_cidade, min, l->itens[imin].timestamp, max, l->itens[imax].timestamp, soma/l->total);
}

int main() {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    DadosCidade caxias, bento;
    inicializar_cidade(&caxias, "Caxias do Sul");
    inicializar_cidade(&bento, "Bento Gonçalves");

    pthread_t t1, t3;
    ThreadArgs args = {&caxias, &bento, "mqtt_senzemo_cx_bg.json"};

    pthread_create(&t1, NULL, thread_leitura, &args);
    pthread_join(t1, NULL); // Espera ler antes de calcular

    pthread_create(&t3, NULL, thread_log, NULL);
    pthread_join(t3, NULL);

    
    printf("============================================================\n");
    printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n\n");

    printf("TEMPERATURA (°C)\n-----------------------------------------------------------------------------------------------\n");
    mostrar_estatisticas(&caxias, &caxias.temp);
    mostrar_estatisticas(&bento, &bento.temp);

    printf("\nBATERIA\n------------------------------------------------------------\n");
    DadosCidade *cids[2] = {&caxias, &bento};
    for(int i=0; i<2; i++){
        if(cids[i]->bateria.total > 0){
            double v_ini = cids[i]->bateria.itens[0].valor;
            double v_fim = cids[i]->bateria.itens[cids[i]->bateria.total-1].valor;
            printf("%-18s | Inicial: %.2fV | Final: %.2fV | Consumo: %.2fV\n", 
                   cids[i]->nome_cidade, v_ini, v_fim, v_ini - v_fim);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    double tempo = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    
    printf("\nDESEMPENHO\n------------------------------------------------------------\n");
    printf("Tempo total de execução: %.4f segundos\n", tempo);
    printf("============================================================\n");

    return 0;
}