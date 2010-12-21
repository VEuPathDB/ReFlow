package ReFlow::DataSource::DataSources;

use strict;

use ReFlow::DataSource::DataSource;

use XML::Simple;
use Data::Dumper;

# this is a separate .pm file because it is of general utility

sub new {
  my ($class, $resourcesXmlFile, $properties) = @_;

  my $self = {};

  bless($self,$class);

  $self->{resourcesXmlFile} = "$ENV{GUS_HOME}/lib/xml/datasources/$resourcesXmlFile";

  $self->_parseXmlFile($self->{resourcesXmlFile}, $properties);

  return $self;
}

sub getXmlFile {
  my ($self) = @_;
  return $self->{resourcesXmlFile};
}

sub getDataSourceNames {
  my ($self) = @_;

  my $resources = $self->{data}->{resource};

  return keys(%$resources);
}

sub getDataSource {
    my ($self, $dataSourceName) = @_;

    die "can't find resource '$dataSourceName' in xml file $self->{resourcesXmlFile}"
      unless $self->{data}->{resource}->{$dataSourceName};

    return ReFlow::DataSource::DataSource->new($dataSourceName, $self->{data}->{resource}->{$dataSourceName}, $self);
}

sub _parseXmlFile {
  my ($self, $resourcesXmlFile, $properties) = @_;

  my $xmlString = $self->_substituteMacros($resourcesXmlFile, $properties);
  my $xml = new XML::Simple();
  $self->{data} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','unpack', 'getAndUnpackOutput', 'resource']) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\n" if($@);
}

sub _substituteMacros {
  my ($self, $xmlFile, $props) = @_;

  my $xmlString;
  open(FILE, $xmlFile) || die "Cannot open resources XML file '$xmlFile'\n";
  while (<FILE>) {
    my $line = $_;
    my @macroKeys = /\@\@([\w.]+)\@\@/g;   # allow keys of the form nrdb.release
    foreach my $macroKey (@macroKeys) {
      my $val = $props->getProp($macroKey);
      die "Invalid macro '\@\@$macroKey\@\@' in xml file $xmlFile" unless defined $val;
      $line =~ s/\@\@$macroKey\@\@/$val/g;
    }
    $xmlString .= $line;
  }
  return $xmlString;
}

sub validateWdkReference {
  my ($self, $project, $record_class, $type, $name) = @_;

  $self->getWdkModel($project);

  # validate record class
  $record_class =~ /^(\S+)\.(\S+)$/ || die "invalid record class name '$record_class'";
  my $recordClassSet = $1;
  my $recordClass = $2;
  return 0 unless $self->{wdkModel}->{recordClassSets}->{$recordClassSet};
  return 0 unless $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$recordClass};

  # validate question
  if ($type eq 'question') {
    $name =~ /^(\S+)\.(\S+)$/ || die "invalid question name '$name'";
    return 0 unless $self->{wdkModel}->{questionSets}->{$1};
    return $self->{wdkModel}->{questionSets}->{$1}->{$2};
  }

  # validate attribute
  elsif ($type eq 'attribute') {
    return 0 unless $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$recordClass}->{attributes};
    return $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$recordClass}->{attributes}->{$name};
  }

  # validate table
  elsif ($type eq 'table') {
    return 0 unless $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$recordClass}->{tables};
    return $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$recordClass}->{tables}->{$name};
  }

  else {
    die "invalid reference type";
  }

}

sub getWdkModel {
  my ($self, $project) = @_;

  return if $self->{wdkModel};  # already got it

  $self->{wdkModel}->{recordClassSets} = {};
  $self->{wdkModel}->{questionSets} = {};

  my $recordClassSet;
  my $recordClass;
  my $questionSet;
  my $gatheringAttributes;
  my $gatheringTables;
  my $gatheringQuestions;

  my $cmd = "wdkXml -model $project";
  open(WDKXML, "$cmd|") || die "Can't run '$cmd'";

  while (<WDKXML>) {
    chomp;
    if (/^RecordClassSet\: name=\'(.*)\'/) {
      $recordClassSet = $1;
    } elsif (/^Record\: name=\'(.*)\'/) {
      $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$1} = {};
      $recordClass = $self->{wdkModel}->{recordClassSets}->{$recordClassSet}->{$1};
    } elsif (/^--- Attributes/) {
      $gatheringAttributes = 1;
    } elsif (/^--- Tables/) {
      $gatheringTables = 1;
      $gatheringAttributes = 0;
    } elsif (/^$/ || /^\s/) {
      $gatheringTables = 0;
      $gatheringAttributes = 0;
    } elsif (/^QuestionSet: name=\'(.*)\'/) {
      $self->{wdkModel}->{questionSets}->{$1} = {};
      $questionSet = $self->{wdkModel}->{questionSets}->{$1};
    } elsif (/^Question: name=\'(.*)\'/) {
      $questionSet->{$1} = 1;
    } elsif ($gatheringAttributes) {
      $recordClass->{attributes}->{$_} = 1
    } elsif ($gatheringTables) {
      $recordClass->{tables}->{$_} = 1
    }
  }
}


1;
