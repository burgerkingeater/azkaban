CREATE TABLE azkaban.execution_dependencies(
  trigger_instance_id varchar(64),
  dep_name varchar(128),
  starttime datetime not null,
  endtime datetime,
  dep_status tinyint not null,
  cancelleation_cause tinyint not null,

  project_id int(11) not null,
  project_version int(11) not null,
  flow_id varchar(128) not null,
  flow_version int(11) not null,

  flow_exec_id int(11) not null,
  primary key(trigger_instance_id, dep_name)
  #CONSTRAINT fk1 FOREIGN KEY (project_id, project_version, flow_id)
  #REFERENCES flow_triggers(project_id, project_version, flow_id)
  #CONSTRAINT fk2 FOREIGN KEY (flow_execution_id)
  #REFERENCES execution_flows(exec_id)
);