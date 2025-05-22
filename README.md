# KafkaProject_5027231020
| Nama                       | NRP        |
| -------------------------- | ---------- |
| Acintya Edria Sudarsono    | 5027231020 |

## Daftar Isi
1.  [Prasyarat](#️-prasyarat)
2.  [Struktur Direktori](#-struktur-direktori)
3.  [Langkah-langkah](#-langkah-langkah)
4.  [Dokumentasi & Output](#-dokumentasi-&-output)

## Prasyarat
Sebelum memulai, pastikan telah terinstal:
1.  **Java Development Kit (JDK):** Versi 8 atau 11 direkomendasikan. Pastikan `JAVA_HOME` sudah di-set.
2.  **Apache Kafka:**
    *   Download dari [kafka.apache.org](https://kafka.apache.org/downloads).
    *   Ekstrak ke direktori (misal `C:\kafka`).
3.  **Apache Spark:**
    *   Download versi pre-built for Hadoop dari [spark.apache.org](https://spark.apache.org/downloads.html).
    *   Ekstrak ke direktori (misal `C:\spark`).
    *   Set `SPARK_HOME` environment variable.
    *   Tambahkan `%SPARK_HOME%\bin` ke `PATH`.
4.  **`winutils.exe` dan `hadoop.dll` (Khusus Windows):**
    *   Download `winutils.exe` dan `hadoop.dll` yang sesuai dengan versi Hadoop yang digunakan Spark Anda (biasanya tertera di nama file download Spark, misal Hadoop 3.x).
    *   Buat direktori `C:\hadoop\bin` (atau nama lain).
    *   Letakkan `winutils.exe` dan `hadoop.dll` di `C:\hadoop\bin`.
    *   Set `HADOOP_HOME` environment variable ke `C:\hadoop`.
    *   Tambahkan `%HADOOP_HOME%\bin` ke `PATH`.
5.  **Python:**
    *   Versi 3.7+ direkomendasikan.
    *   Pastikan Python ada di `PATH`.
    *   Install library yang dibutuhkan:
        ```bash
        pip install pyspark kafka-python
        ```

## Struktur Direktori
```
kafka_pbl_logistik/
├── producers/
│ ├── producer_suhu.py
│ └── producer_kelembaban.py
├── spark_app/
│ └── consumer_processor.py
└── README.md
```

## Langkah-langkah

### 1. Persiapan Apache Kafka

**a. Jalankan Zookeeper:**
Buka Command Prompt (CMD), navigasi ke direktori Kafka Anda (misal `C:\kafka`), lalu jalankan:
```
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```
Biarkan tetap berjalan.

**b. Jalankan Kafka Server (Broker):**
Buka CMD baru, navigasi ke direktori Kafka, lalu jalankan:
```
bin\windows\kafka-server-start.bat config\server.properties
```
Catatan: Pastikan log.dirs di config/server.properties menggunakan path Windows yang valid (misal log.dirs=C:/kafka/kafka-logs).
Biarkan tetap berjalan.

### 2. Buat Topik Kafka
Buka CMD baru, navigasi ke direktori Kafka Anda, lalu jalankan:

- Buat topik untuk sensor suhu
```
bin\windows\kafka-topics.bat --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
- Buat topik untuk sensor kelembaban
```
bin\windows\kafka-topics.bat --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Verifikasi topik:
```
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### 3. Jalankan Producer Kafka
- a. Producer Suhu:
Buka CMD baru, navigasi ke direktori producers/, lalu jalankan:
```
python producer_suhu.py
```
Biarkan berjalan.

- b.  Producer Kelembaban:
Buka CMD baru, navigasi ke direktori producers/, lalu jalankan:
```
python producer_kelembaban.py
```
Biarkan CMD ini berjalan.

### 4. Jalankan Consumer dan Processor PySpark
- a. Persiapan Direktori Checkpoint (jika menggunakan):
Kode consumer_processor.py menggunakan checkpointLocation. Buat direktori yang akan digunakan, misal D:\spark_checkpoints. Pastikan path ini ada dan Spark memiliki izin tulis.
```
D:/spark_checkpoints/peringatan_suhu_checkpoint
D:/spark_checkpoints/peringatan_kelembaban_checkpoint
D:/spark_checkpoints/gabungan_checkpoint
```
Sesuaikan path di consumer_processor.py jika Anda menggunakan lokasi lain.

- b. Jalankan Aplikasi Spark:
Buka CMD baru, navigasi ke direktori spark_app/ (atau direktori utama proyek jika consumer_processor.py ada di sana), lalu jalankan:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer_processor.py
```
Sesuaikan versi package Kafka (3.5.0) jika Anda menggunakan versi Spark yang berbeda.

Pastikan SPARK_HOME dan HADOOP_HOME (dengan winutils.exe) sudah dikonfigurasi dengan benar di environment variables.

## Dokumentasi & Output
### Jalankan Zookeeper
![image](https://github.com/user-attachments/assets/fbe08db6-f86d-4c8f-bde7-109aba246d4f)

### Jalankan Kafka Server
![image](https://github.com/user-attachments/assets/208625d8-0b28-4950-b69c-14bfa44886af)

### Buat Topik sensor
![image](https://github.com/user-attachments/assets/0890ce55-975a-4d1e-b348-fbeb838af606)

### Jalankan producer_suhu.py & producer_kelembaban.py
![image](https://github.com/user-attachments/assets/0c8711a8-b160-4372-b18b-671f44909c7f)


### Output
Output akan muncul secara periodik dalam batch.
![image](https://github.com/user-attachments/assets/0c8711a8-b160-4372-b18b-671f44909c7f)

![image](https://github.com/user-attachments/assets/92382b17-2655-40c9-aeb1-4e01eb6e0be8)

![image](https://github.com/user-attachments/assets/17e4d061-bafe-478d-8279-61bdf8f8c68d)


