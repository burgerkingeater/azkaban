create table flow_triggers(
  project_id int(11),
  project_version int(11),
  flow_id varchar(128),
  trigger_blob longblob not null,
  primary key(project_id, project_version, flow_id)
);