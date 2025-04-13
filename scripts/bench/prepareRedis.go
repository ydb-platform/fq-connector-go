package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// Режимы наполнения
const (
	MODE_STRING_ONLY = "stringOnly"
	MODE_HASH_ONLY   = "hashOnly"
	MODE_MIXED       = "mixed"

	// Выберите режим наполнения
	MODE = MODE_STRING_ONLY

	// Количество пар в HASH-объекте
	HASH_PAIRS_COUNT = 10

	// Размер случайной строки для STRING (в байтах)
	STRING_VALUE_SIZE = 1024 * 2

	// Размер случайной строки для каждого значения в HASH (в байтах)
	HASH_FIELD_VALUE_SIZE = 10

	// Целевой объём данных (например, 5 Гб)
	TARGET_SIZE_BYTES = 1 * 1024 * 1024 * 1024
)

func randomString(n int, r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	// Создаём локальный генератор случайных чисел
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Fatalf("Ошибка закрытия клиента: %v", err)
		}
	}()

	var insertedBytes int64 = 0
	keyIndex := 0

	const batchSize = 1000 // количество операций в одном пайплайне

	// Запускаем цикл до достижения TARGET_SIZE_BYTES
	for insertedBytes < TARGET_SIZE_BYTES {
		pipe := rdb.Pipeline()
		// Собираем batchSize команд
		for i := 0; i < batchSize && insertedBytes < TARGET_SIZE_BYTES; i++ {
			key := fmt.Sprintf("key:%d", keyIndex)
			switch MODE {
			case MODE_STRING_ONLY:
				value := randomString(STRING_VALUE_SIZE, r)
				pipe.Set(ctx, key, value, 0)
				// Приблизительный расчёт: складываем длины ключа и значения
				insertedBytes += int64(len(key) + len(value))
			case MODE_HASH_ONLY:
				hashData := make(map[string]interface{})
				var hashSize int
				for j := 0; j < HASH_PAIRS_COUNT; j++ {
					field := fmt.Sprintf("field:%d", j)
					val := randomString(HASH_FIELD_VALUE_SIZE, r)
					hashData[field] = val
					hashSize += len(field) + len(val)
				}
				pipe.HSet(ctx, key, hashData)
				insertedBytes += int64(len(key) + hashSize)
			case MODE_MIXED:
				if keyIndex%2 == 0 {
					value := randomString(STRING_VALUE_SIZE, r)
					pipe.Set(ctx, key, value, 0)
					insertedBytes += int64(len(key) + len(value))
				} else {
					hashData := make(map[string]interface{})
					var hashSize int
					for j := 0; j < HASH_PAIRS_COUNT; j++ {
						field := fmt.Sprintf("field:%d", j)
						val := randomString(HASH_FIELD_VALUE_SIZE, r)
						hashData[field] = val
						hashSize += len(field) + len(val)
					}
					pipe.HSet(ctx, key, hashData)
					insertedBytes += int64(len(key) + hashSize)
				}
			default:
				log.Fatalf("Неизвестный режим наполнения: %s", MODE)
			}
			keyIndex++
		}

		// Выполнение пакета команд
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("Ошибка выполнения пайплайна: %v", err)
		}

		// Периодический вывод прогресса каждые 10*batchSize ключей.
		if keyIndex%(batchSize*100) == 0 {
			fmt.Printf("Вставлено ключей: %d, приблизительный объём данных: %.2f Гбайт\n",
				keyIndex, float64(insertedBytes)/1024/1024/1024)
		}
	}

	fmt.Printf("Завершено. Всего вставлено ключей: %d, итоговый приблизительный объём: %.2f Гбайт\n",
		keyIndex, float64(insertedBytes)/1024/1024/1024)
}
