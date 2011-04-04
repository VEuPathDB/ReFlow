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

-- This is oracle and postgresql syntax.  The name of this
-- MUST be _sq concatenated onto the name of the Workflow table.
CREATE SEQUENCE Workflow_sq;


-----------------------------------------------------------

create table WorkflowStep (
  workflow_step_id    number(10),
  workflow_id         number(10),
  name                varchar(200),
  host_machine        varchar(30),
  process_id          number(10),
  state               varchar(30),
  state_handled       number(1),
  last_handled_time   date,
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
  depends_string      varchar(4000),
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

-- This is oracle and postgresql syntax.  The name of this
-- MUST be _sq concatenated onto the name of the WorkflowStep table.
CREATE SEQUENCE WorkflowStep_sq;

exit;
