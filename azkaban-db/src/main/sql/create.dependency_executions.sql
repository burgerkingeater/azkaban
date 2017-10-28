CREATE TABLE dependency_executions(
  exec_id varchar(64),
  dep_name varchar(128),
  starttime datetime not null,
  endtime datetime,
  dep_status tinyint not null,
  project_id int(11) not null,
  project_version int(11) not null,
  flow_id varchar(128) not null,
  primary key(exec_id, dep_name),

  CONSTRAINT fk FOREIGN KEY (project_id, project_version, flow_id)
  REFERENCES flow_triggers(project_id, project_version, flow_id)
);