create table Workflow (
  workflow_id              number(10), 
  name                     varchar(30),  -- name and version are an alternate key
  version                  varchar(30),
  state                    varchar(30),
  host_machine             varchar(30),
  process_id               number(10),
  undo_step_id             number(10),
  initializing_step_table  number(1),
  test_mode                number(1)
);

ALTER TABLE Workflow
ADD CONSTRAINT workflow_pk PRIMARY KEY (workflow_id);

ALTER TABLE Workflow
ADD CONSTRAINT workflow_uniq
UNIQUE (name, version);

CREATE SEQUENCE Workflow_sq;

-----------------------------------------------------------

create table WorkflowStep (
  workflow_step_id    number(10),
  workflow_id         number(10),
  name                varchar(500),
  host_machine        varchar(30),
  process_id          number(10),
  state               varchar(30),
  state_handled       number(1),
  last_handled_time   date,
  skipped             number(1),
  off_line            number(1),
  stop_after          number(1),
  undo_state          varchar(30),
  undo_state_handled  number(1),
  undo_off_line       number(1),
  undo_stop_after     number(1),
  undo_last_handled_time date,
  start_time          date,
  end_time            date,
  step_class          varchar(200),
  params_digest       varchar(100),
  depends_string      clob,
  depth_first_order   number(5)
);

ALTER TABLE WorkflowStep
ADD CONSTRAINT workflow_step_pk PRIMARY KEY (workflow_step_id);

ALTER TABLE WorkflowStep
ADD CONSTRAINT workflow_step_fk1 FOREIGN KEY (workflow_id)
REFERENCES Workflow (workflow_id);

CREATE INDEX WorkflowStep_revix
ON WorkflowStep (workflow_id, workflow_step_id);

ALTER TABLE WorkflowStep
ADD CONSTRAINT workflow_step_uniq
UNIQUE (name, workflow_id);

ALTER TABLE Workflow
ADD CONSTRAINT workflow_fk1 FOREIGN KEY (undo_step_id)
REFERENCES WorkflowStep (workflow_step_id);

CREATE INDEX workflow_revix0
ON Workflow (undo_step_id, workflow_id);

CREATE SEQUENCE WorkflowStep_sq;


---------------------------------------------------------------------------

create table WorkflowStepAlgInvocation (
  workflow_step_alg_inv_id number(10),
  workflow_step_id number(10),
  algorithm_invocation_id number(10)
);

ALTER TABLE WorkflowStepAlgInvocation
ADD CONSTRAINT workflow_step_alg_inv_pk PRIMARY KEY (workflow_step_alg_inv_id);

ALTER TABLE WorkflowStepAlgInvocation
ADD CONSTRAINT workflow_step_alg_inv_fk1 FOREIGN KEY (workflow_step_id)
REFERENCES WorkflowStep (workflow_step_id);

CREATE INDEX WorkflowStepAlgInv_revix1
ON WorkflowStepAlgInvocation (workflow_step_id, workflow_step_alg_inv_id);

-- Optionally have a fk constraint on the alg invocation.  This is how we do it in GUS
--ALTER TABLE WorkflowStepAlgInvocation
--ADD CONSTRAINT workflow_step_alg_inv_fk2 FOREIGN KEY (algorithm_invocation_id)
--REFERENCES core.AlgorithmInvocation (algorithm_invocation_id);

CREATE INDEX WorkflowStepAlgInv_revix2
ON WorkflowStepAlgInvocation (algorithm_invocation_id, workflow_step_alg_inv_id);

ALTER TABLE WorkflowStepAlgInvocation
ADD CONSTRAINT workflow_step_alg_inv_uniq
UNIQUE (workflow_step_id, algorithm_invocation_id);

CREATE SEQUENCE WorkflowStepAlgInvocation_sq;

---------------------------------------------------------------------------

create table WorkflowStepParamValue (
  workflow_step_param_value_id number(10),
  workflow_step_id number(10),
  param_name varchar(100),
  param_value varchar(500)
);

ALTER TABLE WorkflowStepParamValue
ADD CONSTRAINT workflow_step_param_val_pk PRIMARY KEY (workflow_step_param_value_id);

ALTER TABLE WorkflowStepParamValue
ADD CONSTRAINT workflow_step_param_val_fk1 FOREIGN KEY (workflow_step_id)
REFERENCES WorkflowStep (workflow_step_id);

CREATE INDEX WorkflowStepParamValue_revix1
ON WorkflowStepParamValue (workflow_step_id, workflow_step_param_value_id);

ALTER TABLE WorkflowStepParamValue
ADD CONSTRAINT workflow_step_param_value_uniq
UNIQUE (workflow_step_id,param_name);

CREATE SEQUENCE WorkflowStepParamValue_sq;

exit;
