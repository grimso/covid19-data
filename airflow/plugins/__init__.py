from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
import operators
import helpers

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
    ]
    helpers = [helpers.SqlQueries]
