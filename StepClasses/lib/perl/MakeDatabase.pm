package ReFlow::StepClasses::MakeDatabase;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

## make an organism specific database in Postgres



## TODO Check for existing database
## Return error to be able to make it fail
sub run {
  my ($self, $test, $undo) = @_;

  # get parameters
  my $dbName = $self->getParamValue('dbName');
  my $roleSql = "SET ROLE gus_w";

  if($undo){
    my $sql = "DROP DATABASE \"$dbName\"";
    $self->{workflow}->_runSql($roleSql);
    $self->{workflow}->_runSql($sql);
  }else{
    if($test) {
      $self->log("will create database with script $sql");
    }
    else {
      my $sql = "CREATE DATABASE \"$dbName\" WITH TEMPLATE template_gus_apidb";
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


