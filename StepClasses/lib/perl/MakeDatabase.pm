package ReFlow::StepClasses::MakeDatabase;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

## make an organism specific database in Postgres



## TODO Check for existing database
## Return error to be able to make it fail
sub run {
  my ($self, $test, $undo) = @_;

  # Prefix db name with project name and version
  my $dbName = $self->getWorkflowConfig('name') . "_" . $self->getWorkflowConfig('version');
  # it's OK to have no dbName. We'll create a database as projectName_version in that case
  # which is useful for global dbs etc.s
  $dbName .= "_" . $self->getParamValue('dbName') if $dbName;

  my $roleSql = "SET ROLE gus_w";

  if($undo){
    my $sql = "DROP DATABASE \"$dbName\"";
    $self->{workflow}->_runSql($roleSql);
    $self->{workflow}->_runSql($sql);
  }else{
      my $sql = "CREATE DATABASE \"$dbName\" WITH TEMPLATE template_gus_apidb";
    if($test) {
      $self->log("will create database with script $sql");
    }
    else {
      $self->log("creating database with script $sql");
      $self->{workflow}->_runSql($roleSql);
      $self->{workflow}->_runSql($sql);
    }
  }
}

sub getParamsDeclaration {
  return (
    'dbName',
  );
}

sub getConfigDeclaration {
  return (
    # [name, default, description]
    # ['', '', ''],
  );
}


