# nf-synapse plugin 
 
This project shows how to implement a simple Nextflow plugin named `nf-synapse` that implements the support for Synapse's Entity storage with customized Synapse File System Provider.

## Plugin assets 
                    
- `settings.gradle`
    
    Gradle project settings. 

- `plugins/nf-synapse`
    
    The plugin implementation base directory.

- `plugins/nf-synapse/build.gradle` 
    
    Plugin Gradle build file. Project dependencies should be added here.

- `plugins/nf-synapse/src/resources/META-INF/MANIFEST.MF` 
    
    Manifest file defining the plugin attributes e.g. name, version, etc.
    The attribute `Plugin-Class` declares the plugin main class. This class 
    should extend the base class `nextflow.plugin.BasePlugin` e.g. 
    `nextflow.synapse.SynapsePlugin`.

- `plugins/nf-synapse/src/resources/META-INF/extensions.idx`
    
    This file declares one or more extension classes provided by the plugin. 
    Each line should contain a Java class fully qualified name implementing 
    the interface `org.pf4j.ExtensionPoint` (or a sub-interface).

- `plugins/nf-synapse/src/main` 

    The plugin implementation sources.

- `plugins/nf-synapse/src/test` 
                             
    The plugin unit tests. 

## ExtensionPointS

ExtensionPoint is the basic interface who use nextflow-core to integrate plugins into it.
It's only a basic interface and serves as starting point for more specialized extensions. 

Among others, nextflow-core integrate following sub ExtensionPointS:

- `TraceObserverFactory` to provide a list of TraceObserverS 
- `ChannelExtensionPoint` to enrich the channel with custom methods

In this plugin you can find examples for both of them

## Compile & run unit tests 

Run the following command in the project root directory (ie. where the file `settings.gradle` is located):

    ./gradlew check

## Run and debug plugin in the development environment

To run and test the plugin in the development environment, configure a local Nextflow build 
using the following steps:
1. Create a root folder for the whole project & cd into the folder:
    ````
    mkdir nfSynapseProject && cd nfSynapseProject
    ````
2. Clone this repo: `nf-synapse` while inside `nfSynapseProject` with:
    ````
    git clone https://github.com/Sage-Bionetworks-Workflows/nf-synapse.git
    ````
3. Clone the Nextflow while inside `nfSynapseProject` with:
    ````
    git clone https://github.com/nextflow-io/nextflow.git
    ````
4. Cd into folder `nextflow` & build the local Nextflow source code with:
    ```
    cd nextflow && ./gradlew compile exportClasspath
    ```
5. Cd into folder `nf-synapse` & compile the plugin source code:
    ```
    cd ../nf-synapse && ./gradlew compileGroovy
    ```
6. While inside `nf-synapse`, run Nextflow with `nf-synapse` plugin on the sample file `synapse_file.nf` (created in this repo) using:
    ```
    ./launch.sh run synapse_file.nf -plugins nf-synapse
    ```

## Package, upload and publish

The project should hosted in a GitHub repository whose name should match the name of the plugin,
that is the name of the directory in the `plugins` folder e.g. `nf-synapse` in this project.

Following these step to package, upload and publish the plugin:

1. Create a file named `gradle.properties` in the project root containing the following attributes
   (this file should not be committed in the project repository):

  * `github_organization`: the GitHub organisation the plugin project is hosted
  * `github_username` The GitHub username granting access to the plugin project.
  * `github_access_token`:  The GitHub access token required to upload and commit changes in the plugin repository.
  * `github_commit_email`:  The email address associated with your GitHub account.

2. The following command, package and upload the plugin in the GitHub project releases page:

    ```
    ./gradlew :plugins:nf-synapse:upload
    ```

3. Create a pull request against the [nextflow-io/plugins](https://github.com/nextflow-io/plugins/blob/main/plugins.json) 
  project to make the plugin public accessible to Nextflow app. 

