package ReFlow::DataSource::DataSourceInfos;

use strict;

use ReFlow::DataSource::DataSourceInfo;

use XML::Simple;
use Data::Dumper;

sub new {
  my ($class, $resourceInfoXmlFile, $properties) = @_;

  my $self = {};

  bless($self,$class);

  $self->{resourceInfoXmlFile} = "$ENV{GUS_HOME}/lib/xml/datasources/$resourceInfoXmlFile";

  $self->_parseXmlFile($self->{resourceInfoXmlFile}, $properties);

  return $self;
}

sub getXmlFile {
  my ($self) = @_;
  return $self->{resourceInfoXmlFile};
}

sub getDataSourceNames {
  my ($self) = @_;

  my $resourceInfos = $self->{data}->{resourceInfo};

  return keys(%$resources);
}

sub getDataSourceInfo {
    my ($self, $dataSourceName) = @_;

    die "can't find resourceInfo '$dataSourceName' in xml file $self->{resourceInfoXmlFile}"
      unless $self->{data}->{resourceInfo}->{$dataSourceName};

    return ReFlow::DataSource::DataSourceInfo->new($dataSourceName, $self->{data}->{resourceInfo}->{$dataSourceName}, $self);
}

sub _parseXmlFile {
  my ($self, $resourceInfoXmlFile) = @_;

  my $xml = new XML::Simple();
  my $xmlString = $self->_substituteMacros($resourcesInfoXmlFile, $properties);
  $self->{data} = eval{ $xml->XMLin($xmlString, SuppressEmpty => undef, KeyAttr => 'resource', ForceArray=>['publication','resourceInfo', 'wdkReference']) };
#  print STDERR Dumper $self->{data};
  die "$@\n$xmlString\nerror processing XML file $resourcesXmlFile\n" if($@);
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

# parse the output of the wdkXml program to snatch the names of
# questions, records, tables and attributes
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

sub _substituteConstants {
  my ($self, $xmlFile) = @_;

  my $xmlString;
  open(FILE, $xmlFile) || die "Cannot open resource info XML file '$xmlFile'\n";
  my $constants;
  while (<FILE>) {
    my $line = $_;
    if (/\<constant/) {
	if (/\<constant\s+name\s*=\s*\"\()"\s+value\s*=\s*\"()\"/) {
	    $constants->{$1} = $2;
	} else {
	    die "Can't parse <constant> element on line $. of $xmlFile.  Must be in form of:
    <constant name=\"xxx\" value=\"yyy\"/>\n";
	}
    }
    my @macroKeys = /\$\$([\w.]+)\$\$/g;   # allow keys of the form nrdb.release
    foreach my $macroKey (@macroKeys) {
      my $val = $constants->{$macroKey};
      die "Undefined constant '\$\$$macroKey\$\$' in xml file $xmlFile" unless defined $val;
      $line =~ s/\$\$$macroKey\$\$/$val/g;
    }
    $xmlString .= $line;
  }
  return $xmlString;
}

1;
