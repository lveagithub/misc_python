select cluster_name from system.local;

select * from system.batches;

select * from system_schema.keyspaces;

select * from system_schema.keyspaces where keyspace_name = 'testks';

select id, department, gender, name, salary from testks.employee;