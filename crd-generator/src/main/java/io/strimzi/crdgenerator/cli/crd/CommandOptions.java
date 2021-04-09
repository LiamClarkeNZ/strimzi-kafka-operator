package io.strimzi.crdgenerator.cli.crd;

import static java.lang.Integer.parseInt;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.api.annotations.VersionRange;
import io.strimzi.crdgenerator.CrdGenerator.ConversionStrategy;
import io.strimzi.crdgenerator.CrdGenerator.NoneConversionStrategy;
import io.strimzi.crdgenerator.CrdGenerator.WebhookConversionStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CommandOptions {

    private boolean yaml = false;

    private final LinkedHashMap<String, String> labels = new LinkedHashMap<>();

    VersionRange<KubeVersion> targetKubeVersions = null;

    ApiVersion crdApiVersion = null;

    List<ApiVersion> apiVersions = null;

    VersionRange<ApiVersion> describeVersions = null;

    ApiVersion storageVersion = null;

    Map<String, Class<? extends CustomResource<?, ?>>> classes = new HashMap<>();

    private final ConversionStrategy conversionStrategy;

    private final Consumer<String> errorReporter;


    public CommandOptions(String[] args) throws ClassNotFoundException, IOException, IllegalStateException {
        this(args, System.err::println);
    }

    @SuppressWarnings({ "unchecked", "CyclomaticComplexity", "JavaNCSS", "MethodLength" })
    public CommandOptions(String[] args, Consumer<String> errorReporter) throws ClassNotFoundException, IOException, IllegalStateException {
        this.errorReporter = errorReporter;

        String conversionServiceUrl = null;
        String conversionServiceName = null;
        String conversionServiceNamespace = null;
        String conversionServicePath = null;
        int conversionServicePort = -1;
        String conversionServiceCaBundle = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                switch (arg) {
                    case "--yaml":
                        yaml = true;
                        break;
                    case "--label":
                        i++;
                        int index = args[i].indexOf(":");
                        if (index == -1) {
                            argParseErr("Invalid --label " + args[i]);
                        }
                        labels.put(args[i].substring(0, index), args[i].substring(index + 1));
                        break;
                    case "--target-kube":
                        if (targetKubeVersions != null) {
                            argParseErr("--target-kube can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--target-kube needs an argument");
                        }
                        else {
                            targetKubeVersions = KubeVersion.parseRange(args[++i]);
                        }
                        break;
                    case "--crd-api-version":
                        if (crdApiVersion != null) {
                            argParseErr("--crd-api-version can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--crd-api-version needs an argument");
                        }
                        else {
                            crdApiVersion = ApiVersion.parse(args[++i]);
                        }
                        break;
                    case "--api-versions":
                        if (apiVersions != null) {
                            argParseErr("--api-versions can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--api-versions needs an argument");
                        }
                        else {
                            apiVersions = Arrays.stream(args[++i].split(",")).map(ApiVersion::parse).collect(Collectors.toList());
                        }
                        break;
                    case "--describe-api-versions":
                        if (describeVersions != null) {
                            argParseErr("--describe-api-versions can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--describe-api-versions needs an argument");
                        }
                        else {
                            describeVersions = ApiVersion.parseRange(args[++i]);
                        }
                        break;
                    case "--storage-version":
                        if (storageVersion != null) {
                            argParseErr("--storage-version can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--storage-version needs an argument");
                        }
                        else {
                            storageVersion = ApiVersion.parse(args[++i]);
                        }
                        break;
                    case "--conversion-service-url":
                        if (conversionServiceUrl != null) {
                            argParseErr("--conversion-service-url can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--conversion-service-url needs an argument");
                        }
                        else {
                            conversionServiceUrl = args[++i];
                        }
                        break;
                    case "--conversion-service-name":
                        if (conversionServiceName != null) {
                            argParseErr("--conversion-service-name can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--conversion-service-name needs an argument");
                        }
                        else {
                            conversionServiceName = args[++i];
                        }
                        break;
                    case "--conversion-service-namespace":
                        if (conversionServiceNamespace != null) {
                            argParseErr("--conversion-service-namespace can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--conversion-service-namespace needs an argument");
                        }
                        else {
                            conversionServiceNamespace = args[++i];
                        }
                        break;
                    case "--conversion-service-path":
                        if (conversionServicePath != null) {
                            argParseErr("--conversion-service-path can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--conversion-service-path needs an argument");
                        }
                        else {
                            conversionServicePath = args[++i];
                        }
                        break;
                    case "--conversion-service-port":
                        if (conversionServicePort > 0) {
                            argParseErr("--conversion-service-port can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--conversion-service-port needs an argument");
                        }
                        else {
                            conversionServicePort = parseInt(args[++i]);
                        }
                        break;
                    case "--conversion-service-ca-bundle":
                        if (conversionServiceCaBundle != null) {
                            argParseErr("--conversion-service-ca-bundle can only be specified once");
                        }
                        else if (i >= arg.length() - 1) {
                            argParseErr("--conversion-service-ca-bundle needs an argument");
                        }
                        else {
                            // TODO read file and base64
                            File file = new File(args[++i]);
                            byte[] bundleBytes = Files.readAllBytes(file.toPath());
                            conversionServiceCaBundle = new String(bundleBytes, StandardCharsets.UTF_8);
                            if (!conversionServiceCaBundle.contains("-----BEGIN CERTIFICATE-----")) {
                                throw new IllegalStateException("File " + file + " given by --conversion-service-ca-bundle should be PEM encoded");
                            }
                            conversionServiceCaBundle = Base64.getEncoder().encodeToString(bundleBytes);
                        }
                        break;
                    default:
                        throw new RuntimeException("Unsupported command line option " + arg);
                }
            }
            else {
                String className = arg.substring(0, arg.indexOf('='));
                String fileName = arg.substring(arg.indexOf('=') + 1).replace("/", File.separator);
                Class<?> cls = Class.forName(className);
                if (!CustomResource.class.equals(cls)
                    && CustomResource.class.isAssignableFrom(cls)) {
                    classes.put(fileName, (Class<? extends CustomResource<?, ?>>) cls);
                }
                else {
                    argParseErr(cls + " is not a subclass of " + CustomResource.class.getName());
                }
            }
        }
        if (targetKubeVersions == null) {
            targetKubeVersions = KubeVersion.parseRange("1.11+");
        }
        if (crdApiVersion == null) {
            crdApiVersion = ApiVersion.V1BETA1;
        }
        if (conversionServiceName != null) {
            conversionStrategy =
                    new WebhookConversionStrategy(conversionServiceName, conversionServiceNamespace, conversionServicePath, conversionServicePort, conversionServiceCaBundle);
        }
        else if (conversionServiceUrl != null) {
            conversionStrategy = new WebhookConversionStrategy(conversionServiceUrl, conversionServiceCaBundle);
        }
        else {
            conversionStrategy = new NoneConversionStrategy();
        }
    }


    void argParseErr(String s) {
        errorReporter.accept("CrdGenerator: error: " + s);
    }


    public boolean isYaml() {
        return yaml;
    }


    public LinkedHashMap<String, String> getLabels() {
        return labels;
    }


    public VersionRange<KubeVersion> getTargetKubeVersions() {
        return targetKubeVersions;
    }


    public ApiVersion getCrdApiVersion() {
        return crdApiVersion;
    }


    public List<ApiVersion> getApiVersions() {
        return apiVersions;
    }


    public VersionRange<ApiVersion> getDescribeVersions() {
        return describeVersions;
    }


    public ApiVersion getStorageVersion() {
        return storageVersion;
    }


    public Map<String, Class<? extends CustomResource<?, ?>>> getClasses() {
        return classes;
    }


    public ConversionStrategy getConversionStrategy() {
        return conversionStrategy;
    }
}
