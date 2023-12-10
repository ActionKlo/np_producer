# np_producer

Simple project to send fake data with kafka

### Run the project

Download project
```bash
git clone https://github.com/ActionKlo/np_producer.git
cd np_producer
```

Edit .env.example file with command:

```bash
nano .env.emaxple
```

write your host and save as .env

### Run docker compose

```bash
docker compose up -d
```
### Check the result

Run consumer 

```bash
go run ./cmd/consumer/
```