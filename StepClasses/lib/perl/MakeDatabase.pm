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

  if($undo){
    my $sql = "DROP DATABASE \"$dbName\"";
    $self->{workflow}->_runSql($sql);
  }else{
    my $sql = "CREATE DATABASE \"$dbName\" WITH TEMPLATE template_gus_apidb";
    $self->log("creating database with script $sql");
    $self->{workflow}->_runSql($sql);
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


