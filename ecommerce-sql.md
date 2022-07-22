# SQL - Dataset: theLook eCommerce

#### Fetching the data available on [Google Cloud](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce?hl=es-419), through the BigQuery API.


```python
from google.cloud import bigquery
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
# Create a "Client" object
client = bigquery.Client()
# Construct a reference to the dataset
dataset_ref = client.dataset("thelook_ecommerce", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the dataset
tables = list(client.list_tables(dataset))
bigquery_tables = {}

# Print names of all tables in the dataset and makes a reference for each table
for table in tables:  
    table_name = table.table_id
    bigquery_tables[table_name] = f'`bigquery-public-data.thelook_ecommerce.{table_name}`'
    print(table_name)

```

    Using Kaggle's public dataset BigQuery integration.
    distribution_centers
    events
    inventory_items
    order_items
    orders
    products
    users
    

#### The functions below were created to ease the execution and show the size of the queries.


```python
#Quick sketch for any given table
def sketch_table(table_name):
    # Construct a reference to the selected (first argument) table
    table_ref = dataset_ref.table(table_name)

    # API request - fetch the table
    table = client.get_table(table_ref)

    return client.list_rows(table, max_results=10).to_dataframe()
    #return table.schema
```


```python
def show_query_size():
    # Create a QueryJobConfig object to estimate size of query without running it
    dry_run_config = bigquery.QueryJobConfig(dry_run=True)

    # API request - dry run query to estimate costs
    dry_run_query_job = client.query(query, job_config=dry_run_config)

    print("This query will process {} Megabytes.".format(dry_run_query_job.total_bytes_processed / 1000000))
```


```python

def run_query():
    # Set up the query
    query_job = client.query(query)

    # API request - run the query, and return a pandas DataFrame
    query_df = query_job.to_dataframe()
    return query_df
```

# **Users Analysis**

#### Let's see how many costumers are registered, first and last date records of enrollment and average age


```python
query = f"""
        SELECT COUNT(DISTINCT(id)) AS total_users, 
        MIN(EXTRACT(DATE FROM created_at)) AS first_customer,
        MAX(EXTRACT(DATE FROM created_at)) AS last_customer,
        AVG(age) AS average_age
        FROM {bigquery_tables['users']}
        """
show_query_size()
run_query()
```

    This query will process 2.4 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_users</th>
      <th>first_customer</th>
      <th>last_customer</th>
      <th>average_age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>100000</td>
      <td>2019-01-02</td>
      <td>2022-07-20</td>
      <td>41.01089</td>
    </tr>
  </tbody>
</table>
</div>



#### Sorted customer's total purchase value according to his user id and other data


```python
query = f"""
        WITH orders_details AS (
        SELECT orders.order_id, orders.user_id, orders.num_of_item, order_items.sale_price
        FROM {bigquery_tables['orders']} AS orders 
        INNER JOIN {bigquery_tables['order_items']} AS order_items
        ON orders.order_id = order_items.order_id
        ) , user_total_sales AS (
        SELECT orders_details.user_id,
        SUM(sale_price) AS orders_total_value,
        COUNT(orders_details.order_id) AS num_orders
        FROM orders_details INNER JOIN {bigquery_tables['users']} AS users
        ON orders_details.user_id = users.id
        GROUP BY orders_details.user_id
        )

        SELECT CONCAT(users.first_name, ' ', users.last_name) AS user_full_name,
        users.age AS age,
        EXTRACT(DATE FROM users.created_at) AS enrolment_date,
        sales.num_orders,
        sales.orders_total_value
        FROM user_total_sales AS sales
        INNER JOIN {bigquery_tables['users']} AS users
        ON sales.user_id = users.id
        ORDER BY sales.orders_total_value DESC
        """
show_query_size()
run_query()
```

    This query will process 8.89871 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_full_name</th>
      <th>age</th>
      <th>enrolment_date</th>
      <th>num_orders</th>
      <th>orders_total_value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Gregory Wood</td>
      <td>55</td>
      <td>2021-03-23</td>
      <td>9</td>
      <td>1624.610001</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Hannah Melendez</td>
      <td>48</td>
      <td>2020-05-16</td>
      <td>4</td>
      <td>1598.990000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Kristi Mosley</td>
      <td>38</td>
      <td>2020-09-24</td>
      <td>6</td>
      <td>1488.960001</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Michael Harris</td>
      <td>20</td>
      <td>2020-10-28</td>
      <td>7</td>
      <td>1485.489998</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Maria Mora</td>
      <td>68</td>
      <td>2020-02-18</td>
      <td>2</td>
      <td>1398.000000</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>79989</th>
      <td>Natasha Nguyen</td>
      <td>54</td>
      <td>2020-02-17</td>
      <td>1</td>
      <td>1.510000</td>
    </tr>
    <tr>
      <th>79990</th>
      <td>Barbara Harvey</td>
      <td>37</td>
      <td>2020-02-07</td>
      <td>1</td>
      <td>1.500000</td>
    </tr>
    <tr>
      <th>79991</th>
      <td>Alicia Fox</td>
      <td>32</td>
      <td>2019-07-17</td>
      <td>1</td>
      <td>1.500000</td>
    </tr>
    <tr>
      <th>79992</th>
      <td>Amanda Reed</td>
      <td>68</td>
      <td>2019-03-20</td>
      <td>1</td>
      <td>0.020000</td>
    </tr>
    <tr>
      <th>79993</th>
      <td>Melissa Skinner</td>
      <td>26</td>
      <td>2020-02-04</td>
      <td>1</td>
      <td>0.020000</td>
    </tr>
  </tbody>
</table>
<p>79994 rows × 5 columns</p>
</div>



#### Total sales by gender and country


```python
query = f"""
        WITH orders_details AS (
        SELECT orders.order_id, orders.user_id, orders.num_of_item, order_items.sale_price
        FROM {bigquery_tables['orders']} AS orders 
        INNER JOIN {bigquery_tables['order_items']} AS order_items
        ON orders.order_id = order_items.order_id
        ) 
        
        SELECT users.country, 
        COUNT(CASE WHEN users.gender = 'F' THEN 1 ELSE NULL END) AS female_users,
        COUNT(CASE WHEN users.gender = 'M' THEN 1 ELSE NULL END) AS male_users,
        COUNT(*) AS total_users,
        SUM (CASE WHEN users.gender = 'F' THEN orders_details.sale_price ELSE NULL END) AS female_sales,
        SUM (CASE WHEN users.gender = 'M' THEN orders_details.sale_price ELSE NULL END) AS male_sales,
        SUM(orders_details.sale_price) AS total_sales
        FROM {bigquery_tables['users']} AS users INNER JOIN orders_details 
        ON orders_details.user_id = id
        GROUP BY users.country
        ORDER BY total_sales DESC
        """
show_query_size()
run_query()
```

    This query will process 6.98119 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>female_users</th>
      <th>male_users</th>
      <th>total_users</th>
      <th>female_sales</th>
      <th>male_sales</th>
      <th>total_sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>China</td>
      <td>30713</td>
      <td>30753</td>
      <td>61466</td>
      <td>1.726638e+06</td>
      <td>1.939853e+06</td>
      <td>3.666491e+06</td>
    </tr>
    <tr>
      <th>1</th>
      <td>United States</td>
      <td>20330</td>
      <td>20125</td>
      <td>40455</td>
      <td>1.129611e+06</td>
      <td>1.279319e+06</td>
      <td>2.408930e+06</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Brasil</td>
      <td>13243</td>
      <td>13351</td>
      <td>26594</td>
      <td>7.315910e+05</td>
      <td>8.414481e+05</td>
      <td>1.573039e+06</td>
    </tr>
    <tr>
      <th>3</th>
      <td>South Korea</td>
      <td>4753</td>
      <td>4824</td>
      <td>9577</td>
      <td>2.561930e+05</td>
      <td>3.065225e+05</td>
      <td>5.627155e+05</td>
    </tr>
    <tr>
      <th>4</th>
      <td>France</td>
      <td>4306</td>
      <td>4153</td>
      <td>8459</td>
      <td>2.419381e+05</td>
      <td>2.634972e+05</td>
      <td>5.054353e+05</td>
    </tr>
    <tr>
      <th>5</th>
      <td>United Kingdom</td>
      <td>4259</td>
      <td>3991</td>
      <td>8250</td>
      <td>2.447549e+05</td>
      <td>2.498424e+05</td>
      <td>4.945973e+05</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Germany</td>
      <td>3628</td>
      <td>3933</td>
      <td>7561</td>
      <td>2.094219e+05</td>
      <td>2.436187e+05</td>
      <td>4.530406e+05</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Spain</td>
      <td>3644</td>
      <td>3488</td>
      <td>7132</td>
      <td>2.069624e+05</td>
      <td>2.196962e+05</td>
      <td>4.266585e+05</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Japan</td>
      <td>2343</td>
      <td>2194</td>
      <td>4537</td>
      <td>1.334597e+05</td>
      <td>1.428265e+05</td>
      <td>2.762862e+05</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Australia</td>
      <td>2056</td>
      <td>1941</td>
      <td>3997</td>
      <td>1.183115e+05</td>
      <td>1.216821e+05</td>
      <td>2.399936e+05</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Belgium</td>
      <td>1177</td>
      <td>1172</td>
      <td>2349</td>
      <td>6.772833e+04</td>
      <td>7.475694e+04</td>
      <td>1.424853e+05</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Poland</td>
      <td>212</td>
      <td>206</td>
      <td>418</td>
      <td>1.157204e+04</td>
      <td>1.290652e+04</td>
      <td>2.447856e+04</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Austria</td>
      <td>12</td>
      <td>1</td>
      <td>13</td>
      <td>5.943200e+02</td>
      <td>1.690000e+02</td>
      <td>7.633200e+02</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Deutschland</td>
      <td>5</td>
      <td>3</td>
      <td>8</td>
      <td>2.599800e+02</td>
      <td>1.409900e+02</td>
      <td>4.009700e+02</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Colombia</td>
      <td>1</td>
      <td>4</td>
      <td>5</td>
      <td>5.990000e+00</td>
      <td>3.649900e+02</td>
      <td>3.709800e+02</td>
    </tr>
  </tbody>
</table>
</div>



# **Sales Analysis**

#### Top profiting product categories


```python
query = f"""
        WITH ordered_cost AS (
        SELECT order_items.product_id, order_items.sale_price, inv_items.cost
        FROM {bigquery_tables['order_items']} AS order_items
        INNER JOIN {bigquery_tables['inventory_items']} AS inv_items
        ON order_items.inventory_item_id = inv_items.id
        )
        SELECT products.category AS category,
        COUNT(*) AS total_products,
        SUM(order_items.cost) AS total_cost,
        SUM(products.retail_price) AS total_retail_price,
        SUM(order_items.sale_price) AS total_sale_price,
        SUM(order_items.sale_price) - SUM(order_items.cost) AS profit
        FROM ordered_cost AS order_items
        INNER JOIN {bigquery_tables['products']} AS products
        ON order_items.product_id= products.id
        GROUP BY category
        ORDER BY profit DESC
        """
show_query_size()
run_query()
```

    This query will process 12.972313 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>total_products</th>
      <th>total_cost</th>
      <th>total_retail_price</th>
      <th>total_sale_price</th>
      <th>profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Outerwear &amp; Coats</td>
      <td>9019</td>
      <td>580555.792118</td>
      <td>1.305369e+06</td>
      <td>1.305369e+06</td>
      <td>724813.676701</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jeans</td>
      <td>12662</td>
      <td>667415.877296</td>
      <td>1.247765e+06</td>
      <td>1.247765e+06</td>
      <td>580348.744279</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Sweaters</td>
      <td>11072</td>
      <td>400269.434241</td>
      <td>8.302222e+05</td>
      <td>8.302222e+05</td>
      <td>429952.755766</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Suits &amp; Sport Coats</td>
      <td>5161</td>
      <td>267118.263097</td>
      <td>6.654185e+05</td>
      <td>6.654185e+05</td>
      <td>398300.266071</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Swim</td>
      <td>11434</td>
      <td>333209.048842</td>
      <td>6.573291e+05</td>
      <td>6.573291e+05</td>
      <td>324120.051886</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Fashion Hoodies &amp; Sweatshirts</td>
      <td>11619</td>
      <td>328353.413790</td>
      <td>6.312374e+05</td>
      <td>6.312374e+05</td>
      <td>302883.966259</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Sleep &amp; Lounge</td>
      <td>11342</td>
      <td>269204.273429</td>
      <td>5.570228e+05</td>
      <td>5.570228e+05</td>
      <td>287818.557979</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Shorts</td>
      <td>11307</td>
      <td>260011.948445</td>
      <td>5.187088e+05</td>
      <td>5.187088e+05</td>
      <td>258696.823040</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Accessories</td>
      <td>9918</td>
      <td>170288.376397</td>
      <td>4.251689e+05</td>
      <td>4.251689e+05</td>
      <td>254880.533226</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Active</td>
      <td>8848</td>
      <td>182093.093492</td>
      <td>4.345263e+05</td>
      <td>4.345263e+05</td>
      <td>252433.206744</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Dresses</td>
      <td>5330</td>
      <td>204926.012584</td>
      <td>4.547422e+05</td>
      <td>4.547422e+05</td>
      <td>249816.198632</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Pants</td>
      <td>7146</td>
      <td>198040.831508</td>
      <td>4.314446e+05</td>
      <td>4.314446e+05</td>
      <td>233403.799974</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Tops &amp; Tees</td>
      <td>11725</td>
      <td>272107.495205</td>
      <td>4.861188e+05</td>
      <td>4.861188e+05</td>
      <td>214011.346544</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Intimates</td>
      <td>13337</td>
      <td>239617.689636</td>
      <td>4.507936e+05</td>
      <td>4.507936e+05</td>
      <td>211175.930920</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Blazers &amp; Jackets</td>
      <td>3163</td>
      <td>110622.628652</td>
      <td>2.920566e+05</td>
      <td>2.920566e+05</td>
      <td>181434.001960</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Maternity</td>
      <td>5123</td>
      <td>113413.290116</td>
      <td>2.569461e+05</td>
      <td>2.569461e+05</td>
      <td>143532.819396</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Underwear</td>
      <td>7242</td>
      <td>92591.715869</td>
      <td>1.967960e+05</td>
      <td>1.967960e+05</td>
      <td>104204.234427</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Pants &amp; Capris</td>
      <td>3591</td>
      <td>101878.482601</td>
      <td>1.932567e+05</td>
      <td>1.932567e+05</td>
      <td>91378.188155</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Plus</td>
      <td>4263</td>
      <td>82684.111529</td>
      <td>1.651716e+05</td>
      <td>1.651716e+05</td>
      <td>82487.528746</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Skirts</td>
      <td>2103</td>
      <td>45351.057146</td>
      <td>1.139499e+05</td>
      <td>1.139499e+05</td>
      <td>68598.843049</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Socks</td>
      <td>6262</td>
      <td>76372.839885</td>
      <td>1.266540e+05</td>
      <td>1.266540e+05</td>
      <td>50281.140199</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Suits</td>
      <td>1061</td>
      <td>74984.887083</td>
      <td>1.238538e+05</td>
      <td>1.238538e+05</td>
      <td>48868.862984</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Socks &amp; Hosiery</td>
      <td>3731</td>
      <td>25223.162176</td>
      <td>6.266676e+04</td>
      <td>6.266676e+04</td>
      <td>37443.597747</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Leggings</td>
      <td>3201</td>
      <td>52696.570940</td>
      <td>8.784658e+04</td>
      <td>8.784658e+04</td>
      <td>35150.008845</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Jumpsuits &amp; Rompers</td>
      <td>936</td>
      <td>22055.170683</td>
      <td>4.145442e+04</td>
      <td>4.145442e+04</td>
      <td>19399.249423</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Clothing Sets</td>
      <td>225</td>
      <td>11835.703173</td>
      <td>1.916629e+04</td>
      <td>1.916629e+04</td>
      <td>7330.586843</td>
    </tr>
  </tbody>
</table>
</div>



#### Top profiting products


```python
query = f"""
        WITH ordered_cost AS (
        SELECT order_items.product_id, order_items.sale_price, inv_items.cost
        FROM {bigquery_tables['order_items']} AS order_items
        INNER JOIN {bigquery_tables['inventory_items']} AS inv_items
        ON order_items.inventory_item_id = inv_items.id
        )
        SELECT products.name AS product_name,
        COUNT(*) AS total_products,
        SUM(order_items.cost) AS total_cost,
        SUM(products.retail_price) AS total_retail_price,
        SUM(order_items.sale_price) AS total_sale_price,
        SUM(order_items.sale_price) - SUM(order_items.cost) AS profit
        FROM ordered_cost AS order_items
        INNER JOIN {bigquery_tables['products']} AS products
        ON order_items.product_id = products.id
        GROUP BY product_name
        ORDER BY profit DESC
        """
show_query_size()
run_query()
```

    This query will process 14.086609 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_name</th>
      <th>total_products</th>
      <th>total_cost</th>
      <th>total_retail_price</th>
      <th>total_sale_price</th>
      <th>profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NIKE WOMEN'S PRO COMPRESSION SPORTS BRA *Outst...</td>
      <td>19</td>
      <td>8365.391995</td>
      <td>17157.00</td>
      <td>17157.00</td>
      <td>8791.608005</td>
    </tr>
    <tr>
      <th>1</th>
      <td>The North Face Apex Bionic Soft Shell Jacket -...</td>
      <td>16</td>
      <td>6599.124027</td>
      <td>14448.00</td>
      <td>14448.00</td>
      <td>7848.875973</td>
    </tr>
    <tr>
      <th>2</th>
      <td>The North Face Denali Down Womens Jacket 2013</td>
      <td>13</td>
      <td>5473.986013</td>
      <td>11739.00</td>
      <td>11739.00</td>
      <td>6265.013987</td>
    </tr>
    <tr>
      <th>3</th>
      <td>The North Face Apex Bionic Mens Soft Shell Ski...</td>
      <td>10</td>
      <td>4198.950015</td>
      <td>9030.00</td>
      <td>9030.00</td>
      <td>4831.049985</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Quiksilver Men's Rockefeller Walkshort</td>
      <td>11</td>
      <td>5194.959000</td>
      <td>9933.00</td>
      <td>9933.00</td>
      <td>4738.041000</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>27253</th>
      <td>Allegra K Ladies Full Length Elastic Waist Ski...</td>
      <td>1</td>
      <td>4.406190</td>
      <td>7.61</td>
      <td>7.61</td>
      <td>3.203810</td>
    </tr>
    <tr>
      <th>27254</th>
      <td>Wool Arctic Socks</td>
      <td>2</td>
      <td>3.599960</td>
      <td>5.98</td>
      <td>5.98</td>
      <td>2.380040</td>
    </tr>
    <tr>
      <th>27255</th>
      <td>Classic Tear Drop Mirror Lens Aviator Sunglasses</td>
      <td>2</td>
      <td>1.290000</td>
      <td>3.44</td>
      <td>3.44</td>
      <td>2.150000</td>
    </tr>
    <tr>
      <th>27256</th>
      <td>Set of 2 - Replacement Insert For Checkbook Wa...</td>
      <td>4</td>
      <td>0.709520</td>
      <td>1.96</td>
      <td>1.96</td>
      <td>1.250480</td>
    </tr>
    <tr>
      <th>27257</th>
      <td>Indestructable Aluminum Aluma Wallet - RED</td>
      <td>10</td>
      <td>0.083000</td>
      <td>0.20</td>
      <td>0.20</td>
      <td>0.117000</td>
    </tr>
  </tbody>
</table>
<p>27258 rows × 6 columns</p>
</div>



#### Distribution Centers numbers on sold products


```python
query = f"""
        WITH dist_centers AS (
        SELECT dists_centers_table.name, dists_centers_table.latitude, dists_centers_table. longitude, products.id, products.cost, products.category
        FROM {bigquery_tables['products']} AS products
        INNER JOIN {bigquery_tables['distribution_centers']} AS dists_centers_table
        ON products.distribution_center_id = dists_centers_table.id
        ) 
        SELECT dist_centers.name AS dist_name, 
        dist_centers.latitude AS dist_lat,
        dist_centers.longitude AS dist_lon,
        COUNT(*) AS products_sold, 
        SUM(dist_centers.cost) AS total_cost, 
        SUM(order_items.sale_price) AS total_sale,
        (SUM(order_items.sale_price) - SUM(dist_centers.cost)) AS profit
        FROM {bigquery_tables['order_items']} AS order_items
        INNER JOIN dist_centers
        ON order_items.product_id = dist_centers.id
        GROUP BY dist_name, dist_lat, dist_lon
        ORDER BY profit DESC
        """
show_query_size()
run_query()
```

    This query will process 3.592425 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>dist_name</th>
      <th>dist_lat</th>
      <th>dist_lon</th>
      <th>products_sold</th>
      <th>total_cost</th>
      <th>total_sale</th>
      <th>profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Houston TX</td>
      <td>29.7604</td>
      <td>-95.3698</td>
      <td>22370</td>
      <td>717623.640137</td>
      <td>1.534064e+06</td>
      <td>816440.439819</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Memphis TN</td>
      <td>35.1174</td>
      <td>-89.9711</td>
      <td>23890</td>
      <td>664773.362638</td>
      <td>1.398037e+06</td>
      <td>733263.579677</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Chicago IL</td>
      <td>41.8369</td>
      <td>-87.6847</td>
      <td>24030</td>
      <td>639165.017494</td>
      <td>1.342336e+06</td>
      <td>703171.334250</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mobile AL</td>
      <td>30.6944</td>
      <td>-88.0431</td>
      <td>18223</td>
      <td>601348.823604</td>
      <td>1.228210e+06</td>
      <td>626861.657898</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philadelphia PA</td>
      <td>39.9500</td>
      <td>-75.1667</td>
      <td>16802</td>
      <td>527853.905966</td>
      <td>1.075380e+06</td>
      <td>547525.775198</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Port Authority of New York/New Jersey NY/NJ</td>
      <td>40.6340</td>
      <td>-73.7834</td>
      <td>16595</td>
      <td>460700.523790</td>
      <td>9.534762e+05</td>
      <td>492775.646743</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Los Angeles CA</td>
      <td>34.0500</td>
      <td>-118.2500</td>
      <td>17195</td>
      <td>460544.883955</td>
      <td>9.525513e+05</td>
      <td>492006.438405</td>
    </tr>
    <tr>
      <th>7</th>
      <td>New Orleans LA</td>
      <td>29.9500</td>
      <td>-90.0667</td>
      <td>12822</td>
      <td>376746.316453</td>
      <td>8.005387e+05</td>
      <td>423792.394503</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Savannah GA</td>
      <td>32.0167</td>
      <td>-81.1167</td>
      <td>11955</td>
      <td>397707.869057</td>
      <td>8.112750e+05</td>
      <td>413567.140827</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Charleston SC</td>
      <td>32.7833</td>
      <td>-79.9333</td>
      <td>16939</td>
      <td>336456.826841</td>
      <td>6.798173e+05</td>
      <td>343360.512476</td>
    </tr>
  </tbody>
</table>
</div>



#### Average number of items and Estimated Time Delivery (ETD) per order by country


```python
query = f"""
        SELECT users.country AS country, 
        AVG(orders.num_of_item) AS AVG_num_items, 
        AVG(DATE_DIFF(orders.delivered_at, orders.created_at, HOUR)) AS ETD_hours,
        FROM {bigquery_tables['orders']} AS orders
        INNER JOIN {bigquery_tables['users']} AS users
        ON orders.user_id = users.id
        WHERE orders.status = 'Complete'
        GROUP BY country
        ORDER BY ETD_hours 
        """
show_query_size()
run_query()
```

    This query will process 6.413219 Megabytes.
    




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>AVG_num_items</th>
      <th>ETD_hours</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Deutschland</td>
      <td>2.000000</td>
      <td>41.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Colombia</td>
      <td>1.000000</td>
      <td>76.000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Poland</td>
      <td>1.368421</td>
      <td>90.315789</td>
    </tr>
    <tr>
      <th>3</th>
      <td>United States</td>
      <td>1.431450</td>
      <td>94.436560</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Germany</td>
      <td>1.462257</td>
      <td>94.436576</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Japan</td>
      <td>1.420854</td>
      <td>94.836683</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Brasil</td>
      <td>1.456561</td>
      <td>94.929691</td>
    </tr>
    <tr>
      <th>7</th>
      <td>South Korea</td>
      <td>1.420448</td>
      <td>94.952208</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Spain</td>
      <td>1.446341</td>
      <td>95.048780</td>
    </tr>
    <tr>
      <th>9</th>
      <td>United Kingdom</td>
      <td>1.426275</td>
      <td>95.074074</td>
    </tr>
    <tr>
      <th>10</th>
      <td>China</td>
      <td>1.460440</td>
      <td>95.472843</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Belgium</td>
      <td>1.470588</td>
      <td>95.828877</td>
    </tr>
    <tr>
      <th>12</th>
      <td>France</td>
      <td>1.451220</td>
      <td>96.044715</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Australia</td>
      <td>1.464950</td>
      <td>96.130186</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Austria</td>
      <td>1.000000</td>
      <td>118.500000</td>
    </tr>
  </tbody>
</table>
</div>


