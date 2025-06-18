# ETL-and-ELT-in-Python
# ETL and ELT in python

## ETL (Extract, Transform, Load)

- Traditional data pipeline design pattern
- Processes data in Extract, Transform, Load sequence
- Can handle both tabular and non-tabular data sources
- Implements transformations using Python with pandas

## ELT (Extract, Load, Transform)

- More recent data processing pattern
- Follows Extract, Load, Transform sequence
- Primarily used with data warehouses
- Typically handles tabular data

# 1.**Extracting data**

## 1.1 F**rom structured sources**

- **Reading in parquet files**

```python
import pandas as pd
# Read the parquet file into memory
raw_stock_data = pd.read_parquet("raw_stock_data.parquet", engine="fastparquet")
```

- **Connecting to SQL databases**
    
    

```python
import sqlalchemy
import pandas as pd

# Connection URI: schema_identifier://username:password@host:port/db
connection_uri = "postgresql+psycopg2://repl:password@localhost:5432/market"
db_engine = sqlalchemy.create_engine(connection_uri)

# Query the SQL 
databaseraw_stock_data = pd.read_sql("SELECT * FROM raw_stock_data LIMIT 10", db_engine)

```

## 1.2 Extract non-tabular data

### **Working with APIs and JSON data**

- **Reading JSON files with pandas**

```python
# Read in a JSON file in the format above
raw_stock_data = pd.read_json("raw_stock_data.json", orient="columns")
```

| `orient` 值 | 说明 |
| --- | --- |
| `'split'` | JSON 格式形如：`{"index": [...], "columns": [...], "data": [...]}`。适合结构明确的数据交换。 |
| `'records'` | JSON 是一个列表，列表中每个元素是一个字典，形如：`[{"col1": val1, "col2": val2}, ...]`。每行是一个字典。常见于日志、API 返回等。 |
| `'index'` | JSON 是一个以索引为键、每行数据为值的字典，形如：`{"index1": {"col1": val1, ...}, "index2": ...}`。 |
| `'columns'` | 默认值。JSON 是列为键、每列是一整个字典，形如：`{"col1": {"index1": val1, ...}, "col2": ...}`。 |
| `'values'` | 只有数据部分（二维列表），形如：`[[val1, val2], [val3, val4], ...]`。不包含列名和索引。 |
| `'table'` | 遵循 [pandas 表格化 JSON 格式](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#json)（`schema` + `data`），常用于数据持久化，适合导出再导入。 |
- **Reading JSON files with json**

```python
import json
withopen("raw_stock_data.json", "r") as file:
	# Load the file into a dictionary    
	raw_stock_data = json.load(file)
	
# Confirm the type of the raw_stock_data variable
print(type(raw_stock_data))<class 'dict'>

```

# 2 **Transforming data**

## **2.1 Transforming  structured data with pandas**

### **Filtering records with .loc[]/.iloc[]**

```python
#loc[Boolean Indexing:columns name]
cleaned = raw_stock_data.loc[raw_stock_data["open"] > 0, ["timestamps", "open", "close"]]
```

### A**ltering data types**

```python
# "timestamps" column currectly looks like: "20230101085731"
# Convert "timestamps" column to type datetime
cleaned["timestamps"] = pd.to_datetime(cleaned["timestamps"], format="%Y%m%d%H%M%S")

# "timestamps" column currently looks like: 1681596000011
# Convert "timestamps" column to type datatime
cleaned["timestamps"] = pd.to_datetime(cleaned["timestamps"], unit="ms")
```

### **Filling missing values with pandas**

```python
# Fill NaN values with specific value for each column
clean_stock_data = raw_stock_data.fillna(value={"open": 0, "close": .5}, axis=1)

# Fill NaN value using other columns
raw_stock_data["open"].fillna(raw_stock_data["close"], inplace=True)

```

### **Grouping data with pandas**

```python
# Use Python to group data by ticker, find the mean of the reamining columns
grouped_stock_data = raw_stock_data.groupby(by=["ticker"], axis=0).mean()
```

## 2.2 **Transforming non-tabular data**

- **Iterating over dictionary components**

```python
# Loop over keys
for key in raw_data.keys():   
	 ...
# Loop over values
for value in raw_data.values():    
	  ...
# Loop over keys and values
for key, value in raw_data.items():    
...	  
	  

```

### Use .get(),get value

- **Parsing data from dictionaries**

![image.png](a781c12b-8e84-4b41-9629-8a97d8f717c4.png)

```python
# Call .get() twice to return the nested "open" value
open_price = entry.get("price").get("open", 0)
# Create columns
raw_data.columns = ["timestamps", "open", "close", "volume"]
# Set the index column to be "timestamps"
raw_data.set_index("timestamps")

```

# 3.Persisting data  with pandas

## 3.1 **Loading data to CSV files using pandas**

```python
# Data extraction and transformation
raw_data = pd.read_csv("raw_stock_data.csv")
```

3.2 

```python
stock_data.to_csv("./stock_data.csv", header=True，index=False，sep="|")
```

## 3.2 **Loading data to a SQL database with panda**

```python
# Create a connection object
connection_uri = "postgresql+psycopg2://repl:password@localhost:5432/market"
db_engine = sqlalchemy.create_engine(connection_uri)

# Use the .to_sql() method to persist data to SQL
clean_stock_data.to_sql(    
			name="filtered_stock_data",
			con=db_engine,
			if_exists="append",    
			index=True,    
			index_label="timestamps"
	)

# Pull data written to SQL table
to_validate = pd.read_sql("SELECT * FROM cleaned_stock_data", db_engine)# Validate counts, record equality, etc...

```
