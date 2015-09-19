Three variations in the API

v1: As described in the design doc
  - StreamingDataFrame extends DataFrame
  - sdf.window returns WindowedData
  - sdf.groupBy.window returns WindowedData

v2: Slight variation of the design doc
  - window.groupBy instead of groupBy.window
  - groupBy does not need to change

v3: That proposed by Rxin
  - DataFrame is always bounded data
  - *DataStream*.window returns DataFrame

