# Price monitoring Data Modelling

Project that provided a supermarket chain with price data from its competitors to allow comparisons and analyses related to promotions and supplier relations.

This repo is the DBT Core project used to model the data.

Tables related to:
- Internal prices
- External prices
- Stores
- Products

5 stage tables were used, and the end result was a Star Schema composed by 2 dim tables (Stores and Products) and 1 fact table (Daily Price Comparison).
