create table apidb.Workflow (
  workflow_id              NUMERIC(10),
  name                     varchar(30),  -- name and version are an alternate key
  version                  varchar(30),
  state                    varchar(30),
  host_machine             varchar(30),
  process_id               NUMERIC(10),
  undo_step_id             NUMERIC(10),
  initializing_step_table  NUMERIC(1),
  test_mode                NUMERIC(1)
);

ALTER TABLE apidb.Workflow
ADD CONSTRAINT workflow_pk PRIMARY KEY (workflow_id);

ALTER TABLE apidb.Workflow
ADD CONSTRAINT workflow_uniq
  UNIQUE (name, version);

GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.Workflow TO gus_w;
GRANT SELECT ON apidb.Workflow TO gus_r;

CREATE SEQUENCE apidb.Workflow_sq;

GRANT SELECT ON apidb.Workflow_sq TO gus_r;
GRANT SELECT ON apidb.Workflow_sq TO gus_w;


-----------------------------------------------------------

create table apidb.WorkflowStep (
  workflow_step_id    NUMERIC(10),
  workflow_id         NUMERIC(10),
  name                varchar(500),
  host_machine        varchar(30),
  process_id          NUMERIC(10),
  state               varchar(30),
  state_handled       NUMERIC(1),
  last_handled_time   TIMESTAMP,
  skipped             NUMERIC(1),
  off_line            NUMERIC(1),
  stop_after          NUMERIC(1),
  undo_state          varchar(30),
  undo_state_handled  NUMERIC(1),
  undo_off_line       NUMERIC(1),
  undo_stop_after     NUMERIC(1),
  undo_last_handled_time TIMESTAMP,
  start_time          TIMESTAMP,
  end_time            TIMESTAMP,
  step_class          varchar(200),
  params_digest       varchar(100),
  depends_string      TEXT,
  depth_first_order   NUMERIC(6)
);

ALTER TABLE apidb.WorkflowStep
ADD CONSTRAINT workflow_step_pk PRIMARY KEY (workflow_step_id);

ALTER TABLE apidb.WorkflowStep
ADD CONSTRAINT workflow_step_fk1 FOREIGN KEY (workflow_id)
  REFERENCES apidb.Workflow (workflow_id);

CREATE INDEX WorkflowStep_revix
  ON apidb.WorkflowStep (workflow_id, workflow_step_id);

ALTER TABLE apidb.WorkflowStep
ADD CONSTRAINT workflow_step_uniq
  UNIQUE (name, workflow_id);

ALTER TABLE apidb.Workflow
ADD CONSTRAINT workflow_fk1 FOREIGN KEY (undo_step_id)
  REFERENCES apidb.WorkflowStep (workflow_step_id);

CREATE INDEX workflow_revix0
  ON apidb.Workflow (undo_step_id, workflow_id);

GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.WorkflowStep TO gus_w;
GRANT SELECT ON apidb.WorkflowStep TO gus_r;

CREATE SEQUENCE apidb.WorkflowStep_sq;

GRANT SELECT ON apidb.WorkflowStep_sq TO gus_r;
GRANT SELECT ON apidb.WorkflowStep_sq TO gus_w;

-----------------------------------------------------------

-- this table is not used yet.  might be needed by pilot GUI

create table apidb.WorkflowStepDependency (
  workflow_step_dependency_id NUMERIC(10),
  parent_id NUMERIC(10),
  child_id NUMERIC(10)
);

ALTER TABLE apidb.WorkflowStepDependency
ADD CONSTRAINT workflow_step_d_pk PRIMARY KEY (workflow_step_dependency_id);

ALTER TABLE apidb.WorkflowStepDependency
ADD CONSTRAINT workflow_step_d_fk1 FOREIGN KEY (parent_id)
  REFERENCES apidb.WorkflowStep (workflow_step_id);

CREATE INDEX WorkflowStepDependency_revix1
  ON apidb.WorkflowStepDependency (parent_id, workflow_step_dependency_id);

ALTER TABLE apidb.WorkflowStepDependency
ADD CONSTRAINT workflow_step_d_fk2 FOREIGN KEY (child_id)
  REFERENCES apidb.WorkflowStep (workflow_step_id);

CREATE INDEX WorkflowStepDependency_revix2
  ON apidb.WorkflowStepDependency (child_id, workflow_step_dependency_id);

ALTER TABLE apidb.WorkflowStepDependency
ADD CONSTRAINT workflow_step_d_uniq
  UNIQUE (parent_id, child_id);

GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.WorkflowStepDependency TO gus_w;
GRANT SELECT ON apidb.WorkflowStepDependency TO gus_r;

CREATE SEQUENCE apidb.WorkflowStepDependency_sq;

GRANT SELECT ON apidb.WorkflowStepDependency_sq TO gus_r;
GRANT SELECT ON apidb.WorkflowStepDependency_sq TO gus_w;

---------------------------------------------------------------------------

create table apidb.WorkflowStepAlgInvocation (
  workflow_step_id NUMERIC(10),
  algorithm_invocation_id NUMERIC(10)
);

ALTER TABLE apidb.WorkflowStepAlgInvocation
ADD CONSTRAINT workflow_step_alg_inv_fk1 FOREIGN KEY (workflow_step_id)
  REFERENCES apidb.WorkflowStep (workflow_step_id);

-- ALTER TABLE apidb.WorkflowStepAlgInvocation
-- ADD CONSTRAINT workflow_step_alg_inv_fk2 FOREIGN KEY (algorithm_invocation_id)
--   REFERENCES core.AlgorithmInvocation (algorithm_invocation_id);

ALTER TABLE apidb.WorkflowStepAlgInvocation
ADD CONSTRAINT workflow_step_alg_inv_uniq
  PRIMARY KEY (workflow_step_id, algorithm_invocation_id);

GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.WorkflowStepAlgInvocation TO gus_w;
GRANT SELECT ON apidb.WorkflowStepAlgInvocation TO gus_r;


---------------------------------------------------------------------------

create table apidb.WorkflowStepParamValue (
  workflow_step_param_value_id NUMERIC(10),
  workflow_step_id NUMERIC(10),
  param_name varchar(100),
  param_value varchar(500)
);

ALTER TABLE apidb.WorkflowStepParamValue
ADD CONSTRAINT workflow_step_param_val_pk PRIMARY KEY (workflow_step_param_value_id);

ALTER TABLE apidb.WorkflowStepParamValue
ADD CONSTRAINT workflow_step_param_val_fk1 FOREIGN KEY (workflow_step_id)
  REFERENCES apidb.WorkflowStep (workflow_step_id);

CREATE INDEX WorkflowStepParamValue_revix1
  ON apidb.WorkflowStepParamValue (workflow_step_id, workflow_step_param_value_id);

ALTER TABLE apidb.WorkflowStepParamValue
ADD CONSTRAINT workflow_step_param_value_uniq
  UNIQUE (workflow_step_id,param_name);

GRANT INSERT, SELECT, UPDATE, DELETE ON apidb.WorkflowStepParamValue TO gus_w;
GRANT SELECT ON apidb.WorkflowStepParamValue TO gus_r;

CREATE SEQUENCE apidb.WorkflowStepParamValue_sq;

GRANT SELECT ON apidb.WorkflowStepParamValue_sq TO gus_r;
GRANT SELECT ON apidb.WorkflowStepParamValue_sq TO gus_w;
