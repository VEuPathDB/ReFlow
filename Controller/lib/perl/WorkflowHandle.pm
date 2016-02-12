package ReFlow::Controller::WorkflowHandle;

##
## lite workflow object (a handle on workflow row in db) used in
## workflowstep UI command changing state of a step
##
## (it avoids the overhead and stringency of parsing and validating
## all workflow steps)

use Exporter;
@ISA = qw(Exporter);
our @EXPORT = qw($READY $ON_DECK $FAILED $DONE $RUNNING $START $END);

use strict;
use DBI;
use FgpUtil::Util::PropertySet;
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

sub new {
  my ($class, $homeDir, $dbName) = @_;

  $homeDir =~ s|/+$||;  # remove trailing /

  die "Workflow home dir '$homeDir' does not exist\n" unless -d $homeDir;

  my $login = '';
  my $password = '';

  if ($dbName) {
    my $pos = index($dbName, '@');
    if ($pos != -1) {
      ($login, $dbName) = split('@', $dbName);
      $pos = index($login, '/');
      if ($pos != -1) {
        ($login, $password) = split('/', $login);
      }
    }
    $dbName = "dbi:Oracle:$dbName";
  } else {
    $dbName = '';
  }

  my $self = {
      homeDir => $homeDir,
      gusHome => $ENV{GUS_HOME},
      dbName => $dbName,
      login => $login,
      password => $password,
  };

  bless($self,$class);

  return $self;
}

# construct step subclass that has a run() method (ie, a step class)
sub getRunnableStep {
  my ($self, $stepClassName, $stepName, $stepId) = @_;
  my $stepClass =  eval "{require $stepClassName; $stepClassName->new('$stepName', '$stepId')}";
  $self->error($@) if $@;
  $stepClass->setWorkflow($self);
  return $stepClass;
}

sub getDbh {
    my ($self) = @_;

    my $dbName   = ($self->{dbName}   eq '') ? $self->getGusConfig('dbiDsn') : $self->{dbName};
    my $login    = ($self->{login}    eq '') ? $self->getGusConfig('databaseLogin') : $self->{login};
    my $password = ($self->{password} eq '') ? $self->getGusConfig('databasePassword') : $self->{password};

    if (!$self->{dbh}) {
	$self->{dbh} = DBI->connect($dbName, $login, $password)
	  or $self->error(DBI::errstr);
    }
    return $self->{dbh};
}

# only to be used to run queries against workflow tables
sub _runSql {
    my ($self,$sql) = @_;
    my $dbh = $self->getDbh();
    my $stmt = $dbh->prepare("$sql") or $self->error(DBI::errstr);
    $stmt->execute() or $self->error(DBI::errstr);
    return $stmt;
}

# only to be used to run queries against workflow tables
sub _runSqlQuery_single_array {
    my ($self, $sql) = @_;
    my $stmt = $self->getDbh()->prepare($sql);
    $stmt->execute() or $self->error(DBI::errstr);
    return $stmt->fetchrow_array();
}

sub getWorkflowHomeDir {
    my ($self) = @_;
    return $self->{homeDir};
}

sub getStepId {
    my ($self, $stepName) = @_;
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $id = $self->getId();
    my $sql = "select workflow_step_id
 from $workflowStepTable
 where workflow_id = $id
 and name = '$stepName'";
    my ($stepId) = $self->_runSqlQuery_single_array($sql);
    return $stepId;
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
	FgpUtil::Util::PropertySet->new($gusConfigFile, \@properties, 1);
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
	 ['workflowTable', "", ""],
	 ['workflowStepTable', "", ""],
	 ['workflowStepParamValueTable', "", ""],
	 ['workflowStepTrackingTable', "", ""],
	);

    if (!$self->{workflowConfig}) {
	my $workflowConfigFile = "$self->{homeDir}/config/workflow.prop";
	$self->{workflowConfig} =
	    FgpUtil::Util::PropertySet->new($workflowConfigFile, \@properties);
	my @path = split(/\/+/, $self->{homeDir});
	my $wfPathVersion = pop(@path);
	my $wfPathProject = pop(@path);
	my $wfName = $self->getWorkflowConfig('name');
	my $wfVersion = $self->getWorkflowConfig('version');

	$self->error("Error: in workflow.prop name=$wfName but in the workflow home dir path the project is '$wfPathProject'. These two must be equal.") unless $wfName eq $wfPathProject;
	$self->error("Error: in workflow.prop version=$wfVersion but in the workflow home dir path the version is '$wfPathVersion'. These two must be equal.") unless $wfVersion eq $wfPathVersion;

    }
    return $self->{workflowConfig}->getProp($key);
}

sub error {
    my ($self, $msg) = @_;

   ## confess "$msg\n\n";
    die "$msg\n\n";  # i don't think we need the stack trace.
}


sub getDbState {
  my ($self) = @_;
  if (!$self->{workflow_id}) {
    $self->{name} = $self->getWorkflowConfig('name');
    $self->{version} = $self->getWorkflowConfig('version');
    $self->{workflowTable} = $self->getWorkflowConfig('workflowTable');
    $self->{workflowStepTable} = $self->getWorkflowConfig('workflowStepTable');
    my $sql = "
select workflow_id, state, host_machine, process_id, initializing_step_table, undo_step_id, test_mode
from $self->{workflowTable}
where name = '$self->{name}'
and version = '$self->{version}'
";
    ($self->{workflow_id}, $self->{state}, $self->{host_machine},$self->{process_id},$self->{initializing_step_table}, $self->{undo_step_id},$self->{test_mode})
      = $self->_runSqlQuery_single_array($sql);
    $self->error("workflow '$self->{name}' version '$self->{version}' not in database")
      unless $self->{workflow_id};
  }
}

sub getName {
  my ($self) = @_;
  return $self->getWorkflowConfig('name');
}

sub getStepNamesFromPattern {
    my ($self, $stepNamePattern) = @_;

    $self->getId();
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $sql = 
"SELECT name, state, undo_state
FROM $workflowStepTable
WHERE name like '$stepNamePattern' ESCAPE '!'
AND workflow_id = $self->{workflow_id}
order by depth_first_order";
  return $self->getStepNamesWithSql($sql);
}

sub getStepNamesFromFile {
  my ($self, $file) = @_;

  my @names;
  $self->getId();
  my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
  open(F, $file) || die "Cannot open steps file '$file'";
  while(<F>) {
    next if /^\#/;
    chomp;
    push(@names, "name like '$_'");
  }
  my $namesList = join("\nor ", @names);
  return [] if !scalar(@names);
  my $sql = 
"SELECT name, state, undo_state
FROM $workflowStepTable
WHERE ($namesList)
AND workflow_id = $self->{workflow_id}
order by depth_first_order";
  return $self->getStepNamesWithSql($sql);
}

sub getStepNamesWithSql {
    my ($self, $sql) = @_;

    $self->getId();
    my $result = [];
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $stmt = $self->getDbh()->prepare($sql);
    $stmt->execute() or $self->error(DBI::errstr);
    while (my ($name, $status, $undo_status) = $stmt->fetchrow_array()) {
	push(@$result, [$name, $status, $undo_status]);
    }
    return $result;
}

sub getId {
  my ($self) = @_;

  $self->getDbState();
  return $self->{workflow_id};
}

sub runCmd {
    my ($self, $cmd) = @_;

    my $output = `$cmd`;
    my $status = $? >> 8;
    $self->error("Failed with status $status running: \n$cmd") if ($status);
    return $output;
}


sub getInitOfflineSteps {
    my ($self) = @_;
    return $self->getStepNamesFromFile($self->getWorkflowHomeDir() . '/config/initOfflineSteps');
}

sub getInitStopAfterSteps {
    my ($self) = @_;
    return $self->getStepNamesFromFile($self->getWorkflowHomeDir() . '/config/initStopAfterSteps');
}

############ Static methods ##########

sub parseAlertsFile {
    my ($alertFile) = @_;

    return unless -e $alertFile;
    open(F, $alertFile) || die "Can't open alert file '$alertFile'\n";
    my $alerts;
    while (<F>) {
        chomp;
	my @a = split(/\t/);
	die "Alert file '$alertFile' error on line $.\n" unless scalar(@a) == 2;
	my $regex = $a[0];
	my $maillist = $a[1];
	my @mailaddrs = split(/,\s+/, $maillist);
	map {die "Illegal email address '$_' in $alertFile" unless /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$/i} @mailaddrs;
	$regex =~ s/\s+$//g;  # remove trailing white space
	push(@$alerts, [$regex, \@mailaddrs]);
      }
    return $alerts;
}

sub findAlertEmailList {
    my ($stepName, $alerts) = @_;

    my $whoToMail; # hash of address -> 1
    foreach my $alert (@$alerts) {
	my $regex = $alert->[0];
	my $mailaddrs = $alert->[1];
	if ($stepName =~ /$regex/) {
	    map {$whoToMail->{$_} = 1} @$mailaddrs;
	}
    }
    close(F);
    return keys(%$whoToMail);
}
1;
