package ReFlow::StepClasses::MakeGusConfig;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

## make an organism specific database in Postgres

## Return error to be able to make it fail
sub run {
  my ($self, $test, $undo) = @_;

  # get parameters
  my $dbName = $self->getParamValue('dbName');

  my $gusConfigSource = $self->getGusConfigFile();
  my $gusConfigTarget = $self->getWorkflowDataDir() . "/$dbName/$dbName" . "_gus.config";

  if($undo){
    if (-e $gusConfigTarget){
      $self->runCmd(0, "rm $gusConfigTarget");
    }
  } else{
    my $dbh = $self->getDbh();

    # Get all params from the dbh.
    # This way we don't have to worry about how the dsn string was written and write
    # a complicated parser. Reconstruct both dbiDsn & jdbcDsn afterwards with the new database name
    # and replace it in the gus.config.

    my $dbiDsn= "dbiDsn=dbi:Pg:dbname=$dbName;host=$dbh->{pg_host};port=$dbh->{pg_port}";
    my $jdbcDsn = "jdbcDsn=jdbc:postgresql:\\/\\/$dbh->{pg_host}:$dbh->{pg_port}\\/$dbName";

    my $cmd = "sed -e 's/^dbiDsn.*/$dbiDsn/' -e 's/^jdbcDsn.*/$jdbcDsn/' $gusConfigSource > $gusConfigTarget";
    $self->runCmd($test, $cmd);
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


