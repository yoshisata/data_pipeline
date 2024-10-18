from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

SCHEMAS = {
    'transactions': StructType([
        StructField('transaction_id', StringType(), nullable=True),
        StructField('account_id', StringType(), nullable=True),
        StructField('transaction_date', DateType(), nullable=True),
        StructField('amount', DecimalType(18, 2), nullable=True),
        StructField('transaction_type', StringType(), nullable=True),
    ]),
    'accounts': StructType([
        StructField('account_id', StringType(), nullable=True),
        StructField('customer_id', StringType(), nullable=True),
        StructField('account_type', StringType(), nullable=True),
        StructField('balance', DecimalType(18, 2), nullable=True),
    ]),
    'customers': StructType([
        StructField('customer_id', StringType(), nullable=True),
        StructField('customer_name', StringType(), nullable=True),
        StructField('join_date', DateType(), nullable=True),
    ]),
}
