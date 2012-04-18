package ReFlow::Controller::Base;

use Exporter;
@ISA = qw(Exporter);
our @EXPORT = qw($READY $ON_DECK $FAILED $DONE $RUNNING $START $END);

use strict;
use DBI;
use FgpUtil::Prop::PropertySet;
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
  my ($class, $homeDir, $dbName) = @_;

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
	FgpUtil::Prop::PropertySet->new($gusConfigFile, \@properties, 1);
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
	 ['workflowStepTrackingTable', "", ""],
	);

    if (!$self->{workflowConfig}) {
	my $workflowConfigFile = "$self->{homeDir}/config/workflow.prop";
	$self->{workflowConfig} =
	    FgpUtil::Prop::PropertySet->new($workflowConfigFile, \@properties);
	my @path = split(/\/+/, $wfHomeDir);
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

sub parseAlertsFile {
    my ($self, $alertFile) = @_;

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
    my ($self, $stepName, $alerts) = @_;

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
