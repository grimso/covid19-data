from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
import operators
import data_logic

# Defining the plugin class
class ETLPlugin(AirflowPlugin):
    name = "etl_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CovidApiToS3Operator,
        operators.AirPollutionApiToS3Operator,
        operators.DimensionTableQualityOperator,
    ]
    helpers = [data_logic.SqlQueries]
