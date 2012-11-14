package ReFlow::DatasetLoader::DatasetLoader;

use strict;
use Data::Dumper;

## beware:  this file needs to be cleaned up.  there are three different names here for the same thing:  datasetLoader, resource, datasource

# the correct name is datasetLoader

sub new {
  my ($class, $resourceName, $parsedXml, $dataSources) = @_;

  my $self = {parsedXml => $parsedXml,
              resourceName => $resourceName,
              dataSources => $dataSources,
              version => $parsedXml->{version},
              plugin =>  $parsedXml->{plugin},
              scope =>  $parsedXml->{scope},
              type =>  $parsedXml->{type},
              subType =>  $parsedXml->{subType},
              allowedSubTypes =>  $parsedXml->{allowedSubTypes},
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

# don't resolve TODAY into a date
sub getRawVersion {
    my ($self) = @_;

    return $self->{version};
}

sub getParentResource {
    my ($self) = @_;

    my $parsedXml = $self->getParsedXml();

    my $parentDatasource;
    my $parentResourceName = $parsedXml->{parentDatasetName};

    if ($parentResourceName) {
	$parentDatasource =
	    $self->{dataSources}->getDataSource($parentResourceName);
	$self->error("Can't find parent datasetLoader '$parentResourceName'") unless $parentDatasource;
    } 
    return $parentDatasource;
}


sub getPlugin {
    my ($self) = @_;

    return $self->{plugin};
}

sub getType {
    my ($self) = @_;

    return $self->{type};
}

sub getSubType {
    my ($self) = @_;

    my $allowed = $self->getAllowedSubTypes();
    if ($allowed && $self->{subType}) {
	$self->error("subType in not found in the allowedSubTypes list ($self->{allowedSubTypes})")
	    unless grep(/$self->{subType}/, @$allowed);
    }
    return $self->{subType};
}

sub getAllowedSubTypes {
    my ($self) = @_;
    
    if (!$self->{allowedST} && $self->{allowedSubTypes}) {
	my @a = split(/,/, $self->{allowedSubTypes});
	$self->{allowedST} = \@a;
    }

    return $self->{allowedST};
}

sub getExternalDbIdType {
    my ($self) = @_;

    return $self->{externalDbIdType};
}

sub getExternalDbIdUrl {
    my ($self) = @_;

    my $url = $self->{externalDbIdUrl};
    if ($url) {
	$self->error("externalDbIdUrl must start with http://") unless $url =~ m|http://|;
	$self->error("externalDbIdUrl must contain the macro EXTERNAL_ID_HERE") unless $url =~ /EXTERNAL_ID_HERE/;
    }
    return $url;
}

sub getExternalDbIdUrlUseSecondaryId {
    my ($self) = @_;

    $self->error("externalDbIdUrlUseSecondaryId must be set to either true or false") unless
	!$self->{externalDbIdUrlUseSecondaryId}
    || $self->{externalDbIdUrlUseSecondaryId} =~ /true|false/;
    return $self->{externalDbIdUrlUseSecondaryId} eq 'true';
}

sub getExternalDbIdIsAnAlias {
    my ($self) = @_;

    $self->error("externalDbIdUrlIsAnAlias must be set to either true or false") unless
	!$self->{externalDbIdIsAnAlias}
    || $self->{externalDbIdIsAnAlias} =~ /true|false/;
    return $self->{externalDbIdIsAnAlias} eq 'true';
}

sub getScope {
    my ($self) = @_;
    my $l = $self->{scope};
    $self->error("Invalid scope '$l'.  Must be global, species or organism")
	unless $l eq 'global' || $l eq 'species' || $l eq 'organism';
    return $self->{scope};
}

sub getOrganismAbbrev {
    my ($self) = @_;
    $self->error("Must provide an organismAbbrev= (because scope = '$self->{scope}'")
	unless ($self->{organismAbbrev} || $self->{scope} eq 'global');
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
    $self->error("There is more then one <manualGet>") if ref($self->{manualGet}) eq "ARRAY";

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
    my ($self, $versionFromDb) = @_; # versionFromDb is optional

    my $parent = "";
    my $name = $self->getName();
    my $version =  $self->getVersion();

    my $parsedXml = $self->getParsedXml();

    my $pluginArgs = $parsedXml->{pluginArgs};

    if ($parsedXml->{parentDatasetName}) {

      if ($pluginArgs =~ /\%(RESOURCE_\w+)\%/) {
	my $macro = $1;
	$self->error("Has a parentDatasetName but is using the macro \%$macro\%.  It must use \%PARENT_$macro\% instead");
      }
      $parent = 'PARENT_';
      $name = $self->getParentResource()->getName();
      $version = $self->getParentResource()->getVersion();
    }

    # if the caller is forcing us to use the version from the db.  eg, if the version or parent version was TODAY
    $version = $versionFromDb if $versionFromDb;

    $pluginArgs =~ s/\%${parent}RESOURCE_NAME\%/$name/g;
    $pluginArgs =~ s/\%${parent}RESOURCE_VERSION\%/$version/g;
    $pluginArgs =~ s/\n+/ /g;
    return $pluginArgs;
}

sub error {
    my ($self, $msg) = @_;
    my $xmlFile = $self->{dataSources}->getXmlFile();
    die "Error in DatasetLoader $self->{resourceName} in file $xmlFile:\n$msg";
}

1;
