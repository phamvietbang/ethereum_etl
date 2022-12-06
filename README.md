## Cơ sở dữ liệu
Sử dụng cơ sở dữ liệu mongodb cách cài đặt [tại đây](https://www.mongodb.com/docs/manual/installation/)
## cài đặt môi trường 

* python3 -m venv .venv
* source .venv/bin/activate
* pip3 install -r requirements.txt

## Help
* python3 ethereumetl.py --help
* python3 ethereumetl.py stream --help
## crawl transaction
* python3 ethereumetl.py stream -l "tx_last_synced_block_file" --stream-type transaction -s 23643700 -e 23643900 --output "mongodb://localhost:27017/" --input-db "mongodb://localhost:27017/" -p https://bsc-dataseed1.binance.org/ --collector-id "transactions"
## crawl event
* python3 ethereumetl.py stream -l "event_last_synced_block_file" --stream-type event -s 23643700 -e 23643900 --output "mongodb://localhost:27017/" --input-db "mongodb://localhost:27017/" -p https://bsc-dataseed1.binance.org/ --collector-id "events" --limit-id "transactions"
## tổng hợp lp token
* python3 ethereumetl.py stream -l "lp_last_synced_block_file" --stream-type lp_token -s 23643700 -e 23643900 --output "mongodb://localhost:27017/" --input-db "mongodb://localhost:27017/" -p https://bsc-dataseed1.binance.org/ --collector-id "lp_tokens" --limit-id "transactions"
## tổng hợp lp token holder
* python3 ethereumetl.py stream -l "holder_last_synced_block_file" --stream-type address -s 23643700 -e 23643900 --output "mongodb://localhost:27017/" --input-db "mongodb://localhost:27017/" -p https://bsc-dataseed1.binance.org/ --collector-id "address" --limit-id "events"
