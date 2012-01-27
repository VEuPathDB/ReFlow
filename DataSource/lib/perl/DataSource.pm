package ReFlow::DataSource::DataSource;

use strict;
use Data::Dumper;

sub new {
  my ($class, $resourceName, $parsedXml, $dataSources) = @_;

  my $self = {parsedXml => $parsedXml,
              resourceName => $resourceName,
              dataSources => $dataSources,
              version => $parsedXml->{version},
              plugin =>  $parsedXml->{plugin},
              scope =>  $parsedXml->{scope},
              externalDbIdType =>  $parsedXml->{externalDbIdType},
              externalDbIdUrl =>  $parsedXml->{externalDbIdUrl},
              externalDbIdUrlUseSecondaryId =>  $parsedXml->{externalDbIdUrlUseSecondaryId},
              externalDbIdIsAnAlias =>  $parsedXml->{externalDbIdIsAnAlias},
              organismAbbrev =>  $parsedXml->{organismAbbrev},
              wgetArgs => $parsedXml->{wgetArgs}->{content},
              wgetUrl => $parsedXml->{wgetArgs}->{url},
              manualGet => $parsedXml->{manualGet},
              # NOTE: these 2 are initialized to empty arrayrefs if undef
              unpacks => $parsedXml->{unpack} || [],
              getAndUnpackOutput => $parsedXml->{getAndUnpackOutput} || [],
             };

  bless($self,$class);

  return $self;
}


sub getDataSources {
  my ($self) = @_;

  return $self->{dataSources};
}

sub getParsedXml {
  my ($self) = @_;

  return $self->{parsedXml};
}


sub getName {
    my ($self) = @_;

    return $self->{resourceName};
}

sub getVersion {
    my ($self) = @_;

    if ($self->{version} eq 'TODAY') {
      my @a = localtime(time); $a[5]+=1900; $a[4]++;
      $self->{version} = sprintf("%4d-%2.2d-%2.2d", $a[5],$a[4],$a[3]);
    }

    return $self->{version};
}

sub getParentResource {
    my ($self) = @_;

    my $parsedXml = $self->getParsedXml();

    my $parentDatasource;
    my $parentResourceName = $parsedXml->{parentResource};

    if ($parentResourceName) {
	$parentDatasource =
	    $self->{dataSources}->getDataSource($parentResourceName);
	die "Error: can't find parent resource '$parentResourceName' mentioned in $self->{resourceName}\n" unless $parentDatasource;
    } 
    return $parentDatasource;
}


sub getPlugin {
    my ($self) = @_;

    return $self->{plugin};
}

sub getExternalDbIdType {
    my ($self) = @_;

    return $self->{externalDbIdType};
}

sub getExternalDbIdUrl {
    my ($self) = @_;

    my $url = $self->{externalDbIdUrl};
    if ($url) {
	die "Error:  externalDbIdUrl must start with http://" unless $url =~ m|http://|;
	die "Error:  externalDbIdUrl must contain the macro EXTERNAL_ID_HERE" unless $url =~ /EXTERNAL_ID_HERE/;
    }
    return $url;
}

sub getExternalDbIdUrlUseSecondaryId {
    my ($self) = @_;

    die "Error:  externalDbIdUrlUseSecondaryId must be set to either true or false" unless
	!$self->{externalDbIdUrlUseSecondaryId}
    || $self->{externalDbIdUrlUseSecondaryId} =~ /true|false/;
    return $self->{externalDbIdUrlUseSecondaryId};
}

sub getExternalDbIdIsAlias {
    my ($self) = @_;

    die "Error:  externalDbIdUrlIsAlias must be set to either true or false" unless
	!$self->{externalDbIdIsAlias}
    || $self->{externalDbIdIsAlias} =~ /true|false/;
    return $self->{externalDbIdIsAlias};
}

sub getScope {
    my ($self) = @_;
    my $l = $self->{scope};
    die "Invalid scope '$l' in $self->{resourceName}.  Must be global, species or organism\n"
	unless $l eq 'global' || $l eq 'species' || $l eq 'organism';
    return $self->{scope};
}

sub getOrganismAbbrev {
    my ($self) = @_;
    die "Must provide an organismAbbrev= in $self->{resourceName} (scope = '$self->{scope}')\n" unless ($self->{organismAbbrev} || $self->{scope} eq 'global');
    return $self->{organismAbbrev};
}

sub getWgetArgs {
    my ($self) = @_;

    return $self->{wgetArgs};
}

sub getWgetUrl {
    my ($self) = @_;
    
    return $self->{wgetUrl};
}

sub getManualGet {
    my ($self) = @_;
    die "
There is more then one <manualGet> in resource $self->{resourceName}" if ref($self->{manualGet}) eq "ARRAY";

    return $self->{manualGet};
}

sub getManualFileOrDir {
    my ($self) = @_;

    my $manualGet = $self->getManualGet();
    my $version = $self->getVersion();
    my $resourceName = $self->getName();

    my $fileOrDir = $manualGet->{fileOrDir};
    $fileOrDir =~ s/\%RESOURCE_NAME\%/$resourceName/g;
    $fileOrDir =~ s/\%RESOURCE_VERSION\%/$version/g;
    return $fileOrDir;
}


sub getUnpacks {
    my ($self) = @_;

    if (!$self->{fixedUnpacks}) {
      my $version = $self->getVersion();
      my @unpacks2;
      foreach my $unpacker (@{$self->{unpacks}}) {
	$unpacker =~ s/\%RESOURCE_VERSION\%/$version/g;
	push(@unpacks2, $unpacker);
      }
      $self->{fixedUnpacks} = \@unpacks2;
    }

    return $self->{fixedUnpacks};
}

sub getGetAndUnpackOutputs {
    my ($self) = @_;

    return $self->{getAndUnpackOutput};
}

sub getPluginArgs {
    my ($self) = @_;

    my $parent = "";
    my $name = $self->getName();
    my $version =  $self->getVersion();

    my $parsedXml = $self->getParsedXml();

    my $pluginArgs = $parsedXml->{pluginArgs};

    if ($parsedXml->{parentResource}) {

      if ($pluginArgs =~ /\%(RESOURCE_\w+)\%/) {
	my $macro = $1;
	my $xmlFile = $self->{dataSources}->getXmlFile();
	die "Resource $self->{resourceName} in file $xmlFile has a parentResource but is using the macro \%$macro\%.  It must use \%PARENT_$macro\% instead\n";
      }
      $parent = 'PARENT_';
      $name = $self->getParentResource()->getName();
      $version = $self->getParentResource()->getVersion();
    }
    $pluginArgs =~ s/\%${parent}RESOURCE_NAME\%/$name/g;
    $pluginArgs =~ s/\%${parent}RESOURCE_VERSION\%/$version/g;
    $pluginArgs =~ s/\n+/ /g;
    return $pluginArgs;
}



1;
