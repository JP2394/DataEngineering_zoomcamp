import re 

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f"Preprocessing: rows with zero passengers {data['passenger_count'].isin([0]).sum()}")
    data =data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    def camel_to_snake(column_name):
        return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', column_name).lower()

    #  # Rename columns using the lambda function
    data.columns = map(camel_to_snake, data.columns)


    return data 


@test
def test_output(output, *args):
    assert (output['passenger_count'].isin([0]).sum()==0) & (output['trip_distance'].isin([0]).sum() == 0) , 'there are rides with zero passengers'
    assert ('vendor_id' in output.columns), 'the column is not in the dataset'