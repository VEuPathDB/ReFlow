DROP TABLE WorkflowStepParamValue;
DROP SEQUENCE WorkflowStepParamValue_sq;

DROP TABLE WorkflowStepAlgInvocation;
DROP SEQUENCE WorkflowStepAlgInvocation_sq;

ALTER TABLE Workflow DROP CONSTRAINT workflow_fk1;

DROP TABLE WorkflowStep;
DROP SEQUENCE WorkflowStep_sq;

DROP TABLE Workflow;
DROP SEQUENCE Workflow_sq;
 
exit;
