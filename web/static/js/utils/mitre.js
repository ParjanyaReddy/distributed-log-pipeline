/**
 * MITRE ATT&CK Framework Mapping Service
 */

/**
 * Maps an anomaly detector type to MITRE ATT&CK technique metadata.
 * @param {string} anomalyType
 * @returns {{ code: string, rule: string, name: string, category: string }}
 */
export function getMitreDetails(anomalyType) {
  const normalized = (anomalyType || "").toLowerCase().trim();
  switch (normalized) {
    case "brute_force":
      return {
        code: "T1110.001",
        rule: "R-01",
        name: "BRUTE FORCE CREDENTIAL STUFFING",
        category: "Credential Access",
      };
    case "volume_spike":
      return {
        code: "T1048.003",
        rule: "R-02",
        name: "VOLUME EXFILTRATION SPIKE",
        category: "Exfiltration",
      };
    case "rare_domain":
      return {
        code: "T1071.001",
        rule: "R-03",
        name: "RARE EXTERNAL C2 CONTACT",
        category: "Command & Control",
      };
    case "beaconing":
      return {
        code: "T1071.004",
        rule: "R-04",
        name: "PERIODIC C2 BEACONING",
        category: "Command & Control",
      };
    default:
      return {
        code: "T1078",
        rule: "R-00",
        name: (anomalyType || "ANOMALY").toUpperCase(),
        category: "Threat Activity",
      };
  }
}
