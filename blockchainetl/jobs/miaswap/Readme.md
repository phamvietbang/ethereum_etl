### Code crawl dữ liệu [log](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/jobs/ethereum_etl/log_exporter.py)
### Code crawl dữ liệu [transaction](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/jobs/ethereum_etl/block_transaction_exporter.py)
### Code lấy danh sách lp token của [miaswap](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/jobs/miaswap/export_lp_tokens.py)
* Chưa tìm thấy transaction hay log nào thể hiện create pair nên phải lấy danh sách lp theo pair id. Dữ liệu lưu tại bảng lp_token
### Code lấy danh sách holder của lp token của [miaswap](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/jobs/miaswap/export_lp_token_holders.py)
* Dựa vào transfer log để lấy dữ liệu danh sách những địa chỉ từng sở hữu lp token. Lưu dữ liệu vào bảng holders
### Code lấy lượng balance của một địa chỉ holder với các [lp token](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/jobs/miaswap/export_liquidity_holder.py)
* Lấy danh sách các lp token mà địa chỉ từng hold. Dữ liệu được lấy từ bảng holders
### Code lưu dữ liệu trên [s3](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/streaming/exporter/s3_streaming_exporter.py)
### Code lấy dữ liệu s3 bằng [athena](https://github.com/phamvietbang/ethereum_etl/tree/main/blockchainetl/streaming/exporter/athena_s3_streaming_exporter.py)
* Cấu trúc dữ liệu các bảng được thể hiện trong code tạo bảng của lấy dữ liệu s3 bằng athena
