mysql_col2type = {
    'incident_number': 'INT',
    'exposure_number': 'SMALLINT',
    'suppression_units': 'SMALLINT',
    'suppression_personnel': 'SMALLINT',
    'ems_units': 'SMALLINT',
    'ems_personnel': 'SMALLINT',
    'other_units': 'SMALLINT',
    'other_personnel': 'SMALLINT',
    'estimated_property_loss': 'FLOAT',
    'estimated_contents_loss': 'FLOAT',
    'fire_fatalities': 'SMALLINT',
    'fire_injuries': 'SMALLINT',
    'civilian_fatalities': 'SMALLINT',
    'civilian_injuries': 'SMALLINT',
    'number_of_alarms': 'SMALLINT',
    'floor_of_fire_origin': 'FLOAT',
    'number_of_floors_with_minimum_damage': 'FLOAT',
    'number_of_floors_with_significant_damage': 'FLOAT',
    'number_of_floors_with_heavy_damage': 'FLOAT',
    'number_of_floors_with_extreme_damage': 'FLOAT',
    'number_of_sprinkler_heads_operating': 'FLOAT',
    'incident_date': 'TIMESTAMP',
    'alarm_dttm': 'TIMESTAMP',
    'arrival_dttm': 'TIMESTAMP',
    'id': 'VARCHAR(255)',
    'address': 'VARCHAR(255)',
    'call_number': 'VARCHAR(255)',
    'close_dttm': 'VARCHAR(255)',
    'city': 'VARCHAR(255)',
    'zipcode': 'VARCHAR(255)',
    'battalion': 'VARCHAR(255)',
    'station_area': 'VARCHAR(255)',
    'box': 'VARCHAR(255)',
    'first_unit_on_scene': 'VARCHAR(255)',
    'primary_situation': 'VARCHAR(255)',
    'mutual_aid': 'VARCHAR(255)',
    'action_taken_primary': 'VARCHAR(255)',
    'action_taken_secondary': 'VARCHAR(255)',
    'action_taken_other': 'VARCHAR(255)',
    'detector_alerted_occupants': 'VARCHAR(255)',
    'property_use': 'VARCHAR(255)',
    'area_of_fire_origin': 'VARCHAR(255)',
    'ignition_cause': 'VARCHAR(255)',
    'ignition_factor_primary': 'VARCHAR(255)',
    'ignition_factor_secondary': 'VARCHAR(255)',
    'heat_source': 'VARCHAR(255)',
    'item_first_ignited': 'VARCHAR(255)',
    'human_factors_associated_with_ignition': 'VARCHAR(255)',
    'structure_type': 'VARCHAR(255)',
    'structure_status': 'VARCHAR(255)',
    'fire_spread': 'VARCHAR(255)',
    'no_flame_spead': 'VARCHAR(255)',
    'detectors_present': 'VARCHAR(255)',
    'detector_type': 'VARCHAR(255)',
    'detector_operation': 'VARCHAR(255)',
    'detector_effectiveness': 'VARCHAR(255)',
    'detector_failure_reason': 'VARCHAR(255)',
    'automatic_extinguishing_system_present': 'VARCHAR(255)',
    'automatic_extinguishing_sytem_type': 'VARCHAR(255)',
    'automatic_extinguishing_sytem_perfomance': 'VARCHAR(255)',
    'automatic_extinguishing_sytem_failure_reason': 'VARCHAR(255)',
    'supervisor_district': 'VARCHAR(255)',
    'neighborhood_district': 'VARCHAR(255)',
    'latitude': 'FLOAT',
    'logitude': 'FLOAT'
}

rs_col2type = {
    'incident_number': 'INTEGER',
    'exposure_number': 'SMALLINT',
    'suppression_units': 'SMALLINT',
    'suppression_personnel': 'SMALLINT',
    'ems_units': 'SMALLINT',
    'ems_personnel': 'SMALLINT',
    'other_units': 'SMALLINT',
    'other_personnel': 'SMALLINT',
    'estimated_property_loss': 'DECIMAL(7,2)',
    'estimated_contents_loss': 'DECIMAL(7,2)',
    'fire_fatalities': 'SMALLINT',
    'fire_injuries': 'SMALLINT',
    'civilian_fatalities': 'SMALLINT',
    'civilian_injuries': 'SMALLINT',
    'number_of_alarms': 'SMALLINT',
    'floor_of_fire_origin': 'DECIMAL(7,2)',
    'number_of_floors_with_minimum_damage': 'DECIMAL(7,2)',
    'number_of_floors_with_significant_damage': 'DECIMAL(7,2)',
    'number_of_floors_with_heavy_damage': 'DECIMAL(7,2)',
    'number_of_floors_with_extreme_damage': 'DECIMAL(7,2)',
    'number_of_sprinkler_heads_operating': 'DECIMAL(7,2)',
    'incident_date': 'TIMESTAMP',
    'alarm_dttm': 'TIMESTAMP',
    'arrival_dttm': 'TIMESTAMP',
    'latitude': 'DECIMAL(13,10)',
    'longitude': 'DECIMAL(13,10)',
    'id': 'VARCHAR',
    'address': 'VARCHAR',
    'call_number': 'VARCHAR',
    'close_dttm': 'VARCHAR',
    'city': 'VARCHAR',
    'zipcode': 'VARCHAR',
    'battalion': 'VARCHAR',
    'station_area': 'VARCHAR',
    'box': 'VARCHAR',
    'first_unit_on_scene': 'VARCHAR',
    'primary_situation': 'VARCHAR',
    'mutual_aid': 'VARCHAR',
    'action_taken_primary': 'VARCHAR',
    'action_taken_secondary': 'VARCHAR',
    'action_taken_other': 'VARCHAR',
    'detector_alerted_occupants': 'VARCHAR',
    'property_use': 'VARCHAR',
    'area_of_fire_origin': 'VARCHAR',
    'ignition_cause': 'VARCHAR',
    'ignition_factor_primary': 'VARCHAR',
    'ignition_factor_secondary': 'VARCHAR',
    'heat_source': 'VARCHAR',
    'item_first_ignited': 'VARCHAR',
    'human_factors_associated_with_ignition': 'VARCHAR',
    'structure_type': 'VARCHAR',
    'structure_status': 'VARCHAR',
    'fire_spread': 'VARCHAR',
    'no_flame_spead': 'VARCHAR',
    'detectors_present': 'VARCHAR',
    'detector_type': 'VARCHAR',
    'detector_operation': 'VARCHAR',
    'detector_effectiveness': 'VARCHAR',
    'detector_failure_reason': 'VARCHAR',
    'automatic_extinguishing_system_present': 'VARCHAR',
    'automatic_extinguishing_sytem_type': 'VARCHAR',
    'automatic_extinguishing_sytem_perfomance': 'VARCHAR',
    'automatic_extinguishing_sytem_failure_reason': 'VARCHAR',
    'supervisor_district': 'VARCHAR',
    'neighborhood_district': 'VARCHAR'
}

column_order = ['incident_number', 'exposure_number', 'suppression_units', 
'suppression_personnel', 'ems_units', 'ems_personnel', 'other_units', 
'other_personnel', 'estimated_property_loss', 'estimated_contents_loss', 
'fire_fatalities', 'fire_injuries', 'civilian_fatalities', 'civilian_injuries', 
'number_of_alarms', 'floor_of_fire_origin', 'number_of_floors_with_minimum_damage', 
'number_of_floors_with_significant_damage', 'number_of_floors_with_heavy_damage', 
'number_of_floors_with_extreme_damage', 'number_of_sprinkler_heads_operating', 
'incident_date', 'alarm_dttm', 'arrival_dttm', 'id', 'address', 'call_number', 
'close_dttm', 'city', 'zipcode', 'battalion', 'station_area', 'box', 
'first_unit_on_scene', 'primary_situation', 'mutual_aid', 'action_taken_primary', 
'action_taken_secondary', 'action_taken_other', 'detector_alerted_occupants', 
'property_use', 'area_of_fire_origin', 'ignition_cause', 'ignition_factor_primary', 
'ignition_factor_secondary', 'heat_source', 'item_first_ignited', 
'human_factors_associated_with_ignition', 'structure_type', 'structure_status', 
'fire_spread', 'no_flame_spead', 'detectors_present', 'detector_type', 
'detector_operation', 'detector_effectiveness', 'detector_failure_reason', 
'automatic_extinguishing_system_present', 'automatic_extinguishing_sytem_type', 
'automatic_extinguishing_sytem_perfomance', 'automatic_extinguishing_sytem_failure_reason', 
'supervisor_district', 'neighborhood_district', 'latitude', 'longitude']