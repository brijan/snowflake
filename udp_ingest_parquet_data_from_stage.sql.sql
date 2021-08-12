CREATE OR REPLACE PROCEDURE public.udp_ingest_parquet_data_from_stage(table_name string, schema_name string, stage_name string, path string)
RETURNS STRING
LANGUAGE javascript
AS

$$
var trucate_query = "TRUNCATE TABLE "+ SCHEMA_NAME+"."+TABLE_NAME+";";
var columns ="";
var expression="";
var query_to_inferschema="select LISTAGG(expression, ', ') list , listagg(b.column_name,', ') cln  "+
            "from information_schema.columns b "+
            "LEFT join table( infer_schema( location=>'@"+STAGE_NAME+PATH+"' , file_format=>'parquet_file' ) ) a  "+
            "on UPPER(a.COLUMN_NAME)=b.column_NAME WHERE table_name='"+TABLE_NAME +"' AND TABLE_SCHEMA='"+SCHEMA_NAME +"'  order by ordinal_position;";
            
try{
    
    statement_with_expressions = snowflake.createStatement({sqlText: query_to_inferschema});
    var rs_with_expression = statement_with_expressions.execute();
    
    if (!rs_with_expression.next()) {
            return "Error: Count query failed.";     
    }
    
    exp = rs_with_expression.getColumnValue("LIST");
    columns = rs_with_expression.getColumnValue("CLN");
    
    var copy_query="COPY INTO "+SCHEMA_NAME+"."+TABLE_NAME+"("+ columns +") FROM (SELECT "+ exp  +" FROM @"+STAGE_NAME+PATH+" (file_format =>parquet_file));";
    
    var truncate_statement = snowflake.createStatement({sqlText: trucate_query});
    var final_statement = snowflake.createStatement({sqlText: copy_query});
    
    snowflake.execute ({sqlText: "begin transaction"});
    truncate_statement.execute();
    final_statement.execute();
    snowflake.execute ({sqlText: "commit"});
    
    return 'SUCCESS';
    }
    
catch(err) {
    return "Error: " + err.message;
}
 
$$
;
