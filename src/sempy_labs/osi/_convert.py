import yaml
import json
from sempy_labs.tom import connect_semantic_model



def import_osi(yaml_file):

    data = yaml.safe_load(yaml_file)

    for model in data['semantic_model']:
        model_name = model.get('name')
        #print(json.dumps(model, indent=2))

        for table in model.get('datasets', []):
            table_name = table.get('name')
            source = column.get('source')
            #print(f"Table: {table_name}")

            for column in table.get('fields', []):
                column_name = column.get('name')
                data_type = column.get('type')
                #primary_key = table.get('primary_key')
                #print(f"  Column: {column_name}")

        for relationship in model.get('relationships', []):
            relationship_name = relationship.get('name')
            from_object = relationship.get('from')
            to_object = relationship.get('to')
            rel_type = relationship.get('type')
        
        for measure in model.get('measures', []):
            measure_name = measure.get('name')
            expression = measure.get('expression')
            #print(f"Measure: {measure_name}")

        for dim in model.get('dimensions', []):
            dim_name = dim.get('name')
            dataset = dim.get('dataset')