package GUS::Workflow::Base;

use Exporter;
@ISA = qw(Exporter);
our @EXPORT = qw($READY $ON_DECK $FAILED $DONE $RUNNING $START $END);

use strict;
use DBI;
use CBIL::Util::MultiPropertySet;
use CBIL::Util::PropertySet;
use Carp;

BEGIN {
# allowed states
  our $READY = 'READY';      # my parents are not done yet  -- default state
  our $ON_DECK = 'ON_DECK';  # my parents are done, but there is no slot for me
  our $FAILED = 'FAILED';
  our $DONE = 'DONE';
  our $RUNNING = 'RUNNING';
  our $WAITING_FOR_PILOT = 'WAITING_FOR_PILOT';  # not used yet.

  our $START = 'START';
  our $END = 'END';
}

# methods shared by the perl controller and perl step wrapper.
# any other language implementation would presumably need equivalent code
sub new {
  my ($class, $homeDir) = @_;

  my $self = {
      homeDir => $homeDir,
      gusHome => $ENV{GUS_HOME},
  };

  bless($self,$class);

  return $self;
}

sub getDbh {
    my ($self) = @_;
    if (!$self->{dbh}) {
	$self->{dbh} = DBI->connect($self->getGusConfig('dbiDsn'),
				    $self->getGusConfig('databaseLogin'),
				    $self->getGusConfig('databasePassword'))
	  or $self->error(DBI::errstr);
    }
    return $self->{dbh};
}

sub runSql {
    my ($self,$sql) = @_;
    my $dbh = $self->getDbh();
    my $stmt = $dbh->prepare("$sql") or $self->error(DBI::errstr);
    $stmt->execute() or $self->error(DBI::errstr);
    return $stmt;
}

sub runSqlQuery_single_array {
    my ($self, $sql) = @_;
    my $stmt = $self->getDbh()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchrow_array();
}

sub getWorkflowHomeDir {
    my ($self) = @_;
    return $self->{homeDir};
}

sub getGusConfig {
    my ($self, $key) = @_;

    my @properties = 
	(
	 # [name, default, description]
	 ['dbiDsn', "", ""],
	 ['databaseLogin', "", ""],
	 ['databasePassword', "", ""],
	);

    if (!$self->{gusConfig}) {
      my $gusConfigFile = "$self->{gusHome}/config/gus.config";
      $self->{gusConfig} =
	CBIL::Util::PropertySet->new($gusConfigFile, \@properties, 1);
    }
    return $self->{gusConfig}->getProp($key);
}

sub getWorkflowConfig {
    my ($self, $key) = @_;

    my @properties = 
	(
	 # [name, default, description]
	 ['name', "", ""],
	 ['version', "", ""],
	 ['workflowXmlFile', "", ""],
	 ['clusterServer', "", ""],
	);

    if (!$self->{workflowConfig}) {
      my $workflowConfigFile = "$self->{homeDir}/config/workflow.prop";
      $self->{workflowConfig} =
	CBIL::Util::PropertySet->new($workflowConfigFile, \@properties);

    }
    return $self->{workflowConfig}->getProp($key);
}

sub error {
    my ($self, $msg) = @_;

    confess "$msg\n\n";
}

1;
