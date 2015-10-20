package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import org.junit.Test;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.*;

/**
 * Created by chrishowe-jones on 20/10/15.
 */
public class ParseCBSMessageTest {

    private static final String expectedJSON = "{\"TCLCLCID\":\"NH\",\"TCLCLAPGPNO\":13,\"SEQNUM\":1,\"tLOLOGTYP\":10,\"tLOLOGSCE\":[1]," +
            "\"tLOLGSRCE\":2,\"tLOLODATE\":null,\"tLOPRDTE\":null,\"tLORPDATE\":null,\"tLOBRANCH\":0,\"tLOBRTD\":0," +
            "\"tLONUMBER\":0,\"tLORSUB\":0,\"tLOFORMCD\":0,\"tLOFORMSB\":0,\"tIPTCLCDE\":999,\"tIPTCLASS\":0," +
            "\"tIPCORIND\":0,\"tIPTTST\":123,\"tIPTQUAL\":0,\"tIPTSPACE\":null,\"tIPTID\":0,\"tIPPBR\":2021," +
            "\"tIPPACTP\":0,\"tIPPCDG\":0,\"tIPPSTEM\":987654321,\"ACCNUM\":00000000000000,\"tIPPRMACC\":0,\"tIPDFLUI\":0," +
            "\"tIPLTAM\":0,\"tIPTAM\":10000000000,\"tIPOCLASS\":0,\"tIPOCOR\":0,\"tIPOTTST\":0,\"tIPOTXCLC\":0,\"tIPPRGTP\":0," +
            "\"tIPPRTP\":10,\"tIPPABNCD\":0,\"tIPRGPABR\":0,\"tIPPAHOBR\":0,\"tIPPRGOCD\":0,\"tIPSECBR\":0," +
            "\"tIPSECATP\":0,\"tIPSECCDG\":0,\"tIPSECSTM\":0,\"tIPRCVBR\":0,\"tIPRCVAT\":0,\"tIPRCVCD\":0," +
            "\"tIPRCVST\":0,\"tIPTD\":null,\"tIPLCHOID\":0,\"tHIASDINT\":0,\"tIPPHOIND\":0,\"tIPPROIN\":0,\"tIPPBRIN\":0," +
            "\"tIPPRGIN\":0,\"tIPIBNIN\":0,\"tIPIRGIN\":0,\"tIPREJIN\":1,\"tIPSUSIN\":0,\"tIPEXCIN\":0,\"tIPUNCIN\":0,\"tIPBUSIND\":0," +
            "\"tIPOBSIND\":0,\"tIPINTXNI\":0,\"tIPORIFM\":0,\"tIPDEFTXN\":0,\"tIPDBCRDI\":0,\"tIPCOMMGI\":0,\"tIPACTXNI\":0,\"tIPNSDII\":0," +
            "\"tHIDINDP\":0,\"tIPDINDP\":0,\"tHIACBL\":123456789099,\"tHILTD\":null,\"tHIPBRIND\":0,\"tHIPROIND\":0,\"tHIPIIND\":0,\"tHITAXAMT\":0," +
            "\"tHIEINT\":0,\"tHIF01\":0,\"tHIDCHG\":0,\"tHIWDIN\":0,\"tHIFWIN\":0,\"tHITDORM\":0,\"tHIIAAIN\":0,\"tHIDELPST\":0,\"tHIXSTACC\":0," +
            "\"tHIEXODIN\":0,\"tHIACNAV\":0,\"tHIPRGFUM\":0,\"tHISTOTRM\":0,\"tHIARTERM\":0,\"tHIHNGIN\":0,\"tHICUSSUI\":0,\"tHIRVDTTO\":null," +
            "\"tHIOVDRSN\":0,\"tHITYPSEC\":0,\"tHINTOTI\":0,\"tHILNDAUT\":0,\"tHIEXTOTI\":0,\"tHIF02\":0,\"tHIACPRDN\":0,\"tHIACSTYP\":0,\"tHIPRODNO\":0," +
            "\"tHISUBTYP\":0,\"tHIF03\":0,\"tHICRBALL\":0,\"tHIADVIN\":0,\"tHIGRNTPI\":0,\"tHITARIFF\":0,\"tIPPABNDX\":0,\"tHIBUSTYP\":0,\"tIPPARGDX\":0," +
            "\"tHIMCHGOS\":0,\"tHIEINTAS\":0,\"tIPFCHGST\":0,\"tHIF05\":0,\"tHISHNUM\":0,\"tHICHGCAT\":0,\"tIPROUTED\":0,\"tIPACNCAP\":0,\"tIPCEMRAM\":0," +
            "\"tIPTXNFL7\":0,\"tIPACSTIP\":0,\"tIPTXNFL6\":0,\"tIPRJRTDP\":0,\"tIPTXNFL4\":0,\"tIPTXNFL3\":0,\"tIPTXNFL2\":0,\"tIPTXNFL1\":0," +
            "\"tIPCURCDE\":12,\"tIPSCACII\":0,\"tIPF08\":0,\"tIPPDNOII\":0,\"tIPF09\":0,\"tIPCURCII\":0,\"tIPCRTO\":0,\"tIPDBFDBE\":0,\"tHIUNAINT\":0," +
            "\"tIPDBFLUD\":null,\"tIPHDIAM\":0,\"tIPMKTSEC\":0,\"tIPNOCDUP\":0,\"tIPMKTSEG\":0,\"tIPMKTSGI\":0,\"tIPF24\":\"\",\"tIPF25\":\"\",\"tHIOCHG\":0," +
            "\"tIPF20\":0,\"tIPLWSPB\":\"\",\"tPCPROCID\":\"\",\"tPCLSBTYP\":0,\"tPCTHRDNO\":0,\"tPCACTTME\":0,\"tPCRGSAFF(1)\":0,\"tPCLW\":\"\",\"tTMREJIPT\":0," +
            "\"tTMICTIPT\":0,\"tTMELTIPT\":0,\"tTMPREVTX\":0,\"tTMPRVCMS\":0,\"tTMELIWSI\":0,\"tTMELIWSO\":0,\"tTMELTFPT\":0,\"tTMELTRPT\":0,\"tIPTETIME\":153236," +
            "\"tIPCDATE\":151013,\"tIPTETMLS\":55956914,\"TIME\":\"00:00:00.000\",\"tIPITIME\":0,\"tIPOPBR\":0,\"tIPOACTP\":0,\"tIPOCDG\":0,\"tIPOPSTEM\":0," +
            "\"tIPPRIMAN\":0,\"tIPORTEL\":\"\",\"tIPORBR\":0,\"tIPCBNO\":0,\"tIPBTTXN\":0,\"tIPORPRES\":0,\"tIPPBIN\":0,\"tIPMSIN\":0,\"tIPNBEIN\":0,\"tIPDGIN\":0," +
            "\"tIPCQNIN\":0,\"tIPPREDAT\":0,\"tIP8ACC\":0,\"tIPREJIPM\":0,\"tIPTKTIN\":0,\"tIPCONTXN\":0,\"tIPTRNING\":0,\"tIPATMTXN\":0,\"tIPSPBKCP\":0," +
            "\"tIPITXNSP\":0,\"tIPAUTCBP\":0,\"tIPTXUNAT\":0,\"tIPTREPIN\":0,\"tIPUNSOLM\":0,\"tIPREJMSG\":0,\"tIPEXCMSG\":0,\"tIPCNCTXN\":0,\"tIPUNSTEL\":0," +
            "\"tIPUNSWRK\":0,\"tIPUNSBCH\":0,\"tIPUNSRGN\":0,\"tIPUNSRGT\":0,\"tIPUNSCNC\":0,\"tIPOTSTR\":0,\"tIPINSPIP\":0,\"tIPADMNIP\":0,\"tIPMANIP\":0," +
            "\"tIPSUPIP\":0,\"tIPTELIP\":0,\"tIPOOFFID\":\"\",\"tIPSPORD1\":0,\"tIPSPORD2\":0,\"tIPCLIOR\":0,\"tIPINSPOR\":0,\"tIPADMNOR\":0,\"tIPMANOR\":0," +
            "\"tIPSUPOR\":0,\"tIPTELOR\":0,\"tIPOOFFIN\":0,\"tIPIDVTP\":0,\"tIPISDTP\":0,\"tIPODVTP\":0,\"tIPOSDTP\":0,\"tIPICTIND\":0,\"tIPOBIND\":0,\"tIPTSRCE\":0," +
            "\"tIPTSDEV\":0,\"tIPTSLOCN\":0,\"tIPTSCAP\":0,\"tIPAGRID\":0,\"tIPRGORBR\":0,\"tIPOBNCD\":0,\"tIPCDTSBB\":2015-10-13,\"tIPIWUSID\":\"\",\"tIPTDNO\":0," +
            "\"tIPTDSRNO\":0,\"tIPLONO\":0,\"tIPLOTYP\":0,\"tIPOVOFSI\":\"\",\"tIPOVRDET\":0,\"tIPTRFTYP\":0,\"tIPSCSHBX\":\"\",\"tIPUNISQN\":0,\"tIPF21\":\"\"," +
            "\"tIPIWAPI\":\"\",\"tIPOVOFSN\":0,\"tIPIWCHOI\":\"\",\"tIPOVRPFF\":0,\"tIPIWUIDC\":0,\"tIPIWCOTC\":0,\"tIPSLEF01\":0,\"tIPSECCDE\":0,\"tHIMTYP\":0," +
            "\"tIPLDAPDT\":2015-10-13,\"tIPLWETB\":\"\",\"tIPAVGBL\":0,\"tIPERRMX\":0,\"tIPBSTRNO\":0,\"tIPCBXRQ\":0,\"tIPREFRQ\":0,\"tIPNOTDG\":0,\"tIPDFCHG\":0," +
            "\"tIPIMCHG\":0,\"tIPACNP\":0,\"tIPLGAM\":0,\"tIPCDYSP\":0,\"tIPCHQNA\":0,\"tIPAMP\":0,\"tIPPBDTA\":0,\"tIPNCSFU\":0,\"tIPCTSBT\":0,\"tIPADDRES\":0," +
            "\"tIPCQIN\":0,\"tIPNOTTI\":0,\"tIPNOTDF\":0,\"tIPRAREQ\":0,\"tIPCINICR\":0,\"tIPPRETKO\":0,\"tIPNRMRD\":0,\"tIPCNCORG\":0,\"tIPSPTXNE\":0,\"tIPIICHGC\":0," +
            "\"tIPONSEQP\":0,\"tIPPH2CPR\":0,\"tIPROUTAP\":0,\"tIPREJRDA\":0,\"tIPPMUNMD\":0,\"tIPPM2UMD\":0,\"tIPUNC\":0,\"tIPHBSRAP\":0,\"tIPTAICTR\":0,\"tIPP1APRQ\":0," +
            "\"tIPP3APRQ\":0,\"tIPMPORSP\":0,\"tIPICRNRQ\":0,\"tIPACTP0\":0,\"tIPACTP1\":0,\"tIPACTP2\":0,\"tIPACTP3\":0,\"tIPACTP4\":0,\"tIPACTP5\":0,\"tIPACTP6\":0," +
            "\"tIPACTP7\":0,\"tIPACTP8\":0,\"tIPACTP9\":0,\"tIPWDPENA\":0,\"tIPTXMPRQ\":0,\"tIPSGICTA\":0,\"tIPICCRQ\":0,\"tIPAOBTG\":0,\"tIPASNAT\":0,\"tIPAOBNTG\":0," +
            "\"tIPBIT20\":0,\"tIPSETXN\":0,\"tIPOBSBKS\":0,\"tIPPSACNA\":0,\"tIPATEF\":0,\"tIPCRTOAP\":0,\"tIPBNMRIC\":0,\"tIPBIT14\":0,\"tIPIBRACA\":0,\"tIPAGCGLG\":0," +
            "\"tIPFORTXN\":0,\"tIPFORMOP\":0,\"tIPNPIAA\":0,\"tIPNPDAA\":0,\"tIPNPELAC\":0,\"tIPRNATCH\":0,\"tIPDTSBA\":0,\"tIPIWTXNE\":0,\"tIPLLDPAC\":0," +
            "\"tIPTNPDRM\":0,\"tIPSGIMTN\":0,\"tIPMANPH3\":0,\"tIPFLTID\":1125,\"tIPFLRRC\":0,\"tIPINPID\":0,\"tIPATBINO\":0,\"tIPISOCKD\":0,\"tIPTEFCUS\":\"\"," +
            "\"tIPTEFSEQ\":\"\",\"tIPTEFTIN\":0,\"tIPDBIIN\":0,\"tIPDBR\":0,\"tIPDBRIN\":0,\"tIPDCDG\":0,\"tIPICCENT\":\"\",\"tIPICRUNO\":0,\"tIPPRDNUM\":0," +
            "\"tIPSUBTYP\":0,\"tIPCAVBAL\":0,\"tIPSTOPD\":0,\"tIPACTDY\":0,\"tIPACTDAY\":0,\"tIPARAD\":0,\"tIPERRCT\":0,\"tIPLENPNO\":0,\"tIPDEXPDI\":0,\"tIPDBUCT\":0," +
            "\"tIPPROGIN\":0,\"tIPOBFNCT\":0,\"tIPISRPIN\":0,\"tIPIDF01\":0,\"tIPCRDFMI\":0,\"tIPDEXPDT\":2015-10-13,\"tIPTMTXNO\":0,\"tIPCDSEQN\":0," +
            "\"tIPATAUDT\":2015-10-13,\"tIPKBID1X\":0,\"tIPKBID2X\":0,\"tIPKBLKIN\":\"\",\"tIPKBID1N\":0,\"tIPKBLKIB\":0,\"tIPKBID1B\":0,\"tIPKBID2B\":0," +
            "\"tIPDTLOCT\":2015-10-13,\"tIPTMLOCT\":0,\"tIPATMSWL\":0,\"tIPPOSCNC\":0,\"tIPPOSENM\":0,\"tIPPOSPCC\":0,\"tIPADMTCD\":0,\"tIPPRODNO\":0,\"tIPEPINBK\":\"\"" +
            ",\"tIPPBMINT\":\"\",\"tIPTK2DTA\":\"\",\"tIPCISSID\":\"\",\"tIPT2BRCD\":\"\",\"tIPT2STEM\":\"\",\"tIPT2ACTP\":0,\"tIPT2CDG\":0,\"tIPT2EXDT\":\"\"," +
            "\"tIPT2SERV\":\"\",\"tIPT2CDIN\":0,\"tIPT2STE2\":\"\",\"tIPT2CD2\":0,\"tIPT2EXD2\":\"\",\"tIPT2SER2\":\"\",\"tIPT2ISNO\":\"\",\"tIPT2VFDT\":\"\"," +
            "\"tIPT2CVV\":\"\",\"tIPT2ACBS\":0,\"tIPT2KEED\":\"\",\"tIPT2KEIN\":\"\",\"tIPLNTK2D\":0,\"tIPATSWRL\":0,\"tIPSPDOI\":0,\"tIPISINID\":0,\"tIPAQINID\":0," +
            "\"tIPBCOMNO\":0,\"tIPHBOROI\":0,\"tIPIDF07\":0,\"tIPCBHBOS\":0,\"tIPIDF05\":0,\"tIPIDF02\":\"\",\"tIPISRPFN\":0,\"tIPIDF03\":0,\"tIPIDF04\":0," +
            "\"tIPLWNRID\":\"\",\"tIPIMESA\":\"\",\"tIPECHG\":0,\"tIPELTD\":2015-10-13,\"tIPEEINT\":0,\"tIPEMBL\":0,\"tIPELBL\":0,\"tIPEPLN\":0,\"tIPELWD\":2015-10-13," +
            "\"tIPEAMCEA\":0,\"tIPEAMCAI\":0,\"tIPEBRI\":0,\"tIPEMGN\":0,\"tIPERVD\":2015-10-13,\"tIPEDCL\":0,\"tIPXDTGTD\":2015-10-13,\"tIPXFNRPD\":2015-10-13," +
            "\"tIPXPODLN\":0,\"tIPXRSNOD\":0,\"tIPETPSEC\":0,\"tIPETARIF\":0,\"tIPXININD\":0,\"tIPEIC\":0,\"tIPESDB\":0,\"tIPEAVGB\":0,\"tIPELPPD\":2015-10-13," +
            "\"tIPEBUSTP\":0,\"tIPETRCGO\":0,\"tIPETCVI\":0,\"tIPETRVI\":0,\"tIPETCVV\":0,\"tIPETRVV\":0,\"tIPECRTO\":0,\"tIPESERCG\":0,\"tIPECCRTR\":0,\"tIPECDRTR\":0," +
            "\"tIPEMXVPE\":0,\"tIPEMNVPE\":0,\"tIPEMXVMN\":0,\"tIPEMNVMN\":0,\"tIPECGDTO\":2015-10-13,\"tIPENCGDT\":2015-10-13,\"tIPECGFQ\":0,\"tIPELNAUT\":0," +
            "\"tIPEFRPAY\":0,\"tIPEARRCR\":0,\"tIPEXINT\":0,\"tIPEMGCD\":0,\"tIPETAXCD\":0,\"tIPETERM\":0,\"tIPENOTIR\":0,\"tIPEOCHG\":0,\"tIPEAAC1\":0,\"tIPEAAC2\":0," +
            "\"tIPEAAC3\":0,\"tIPEAAC4\":0,\"tIPEAAC5\":0,\"tIPEAAC6\":0,\"tIPEAAC7\":0,\"tIPEAAC8\":0,\"tIPNCHG\":0,\"tIPNCHGI\":0,\"tIPNLTD\":0,\"tIPNEINT\":0," +
            "\"tIPNMBL\":0,\"tIPNLBL\":0,\"tIPNPLN\":0,\"tIPNLWD\":0,\"tIPNAMCNA\":0,\"tIPNAMCAI\":0,\"tIPNBRI\":0,\"tIPNMGN\":0,\"tIPNRVD\":0,\"tIPNDCL\":0," +
            "\"tIPNDTGTD\":0,\"tIPNFNRPD\":0,\"tIPNPODLN\":0,\"tIPNRSNOD\":0,\"tIPNTPSEC\":0,\"tIPNTARIF\":0,\"tIPNININD\":0,\"tIPNIC\":0,\"tIPNSDB\":0,\"tIPNAVGB\":0," +
            "\"tIPNLPPD\":0,\"tIPNBUSTP\":0,\"tIPNTRCGO\":0,\"tIPNTCVI\":0,\"tIPNTRVI\":0,\"tIPNTCVV\":0,\"tIPNTRVV\":0,\"tIPNCRTO\":0,\"tIPNSERCG\":0,\"tIPNCCRTR\":0," +
            "\"tIPNCDRTR\":0,\"tIPNMXVPE\":0,\"tIPNMNVPE\":0,\"tIPNMXVMN\":0,\"tIPNMNVMN\":0,\"tIPNCGDTO\":0,\"tIPNNCGDT\":0,\"tIPNCGFQ\":0,\"tIPNLNAUT\":0," +
            "\"tIPNFREP\":0,\"tIPNARRCR\":0,\"tIPNEWINT\":0,\"tIPNMGCD\":0,\"tIPNTAXCD\":0,\"tIPNREPTP\":0,\"tIPNTERM\":0,\"tIPNNOTIR\":0,\"tIPNOCHG\":0," +
            "\"tIPNAAC1\":0,\"tIPNAAC2\":0,\"tIPNAAC3\":0,\"tIPNAAC4\":0,\"tIPNAAC5\":0,\"tIPNAAC6\":0,\"tIPNAAC7\":0,\"tIPNAAC8\":0,\"tIPNOCHGI\":0,\"tIPNAAC1I\":0," +
            "\"tIPNAAC2I\":0,\"tIPNAAC3I\":0,\"tIPNAAC4I\":0,\"tIPNAAC5I\":0,\"tIPNAAC6I\":0,\"tIPNAAC7I\":0,\"tIPNAAC8I\":0,\"tIPACAUII\":0,\"tIPACUNII\":0," +
            "\"tIPNWPDFI\":0,\"tIPACAUDI\":0,\"tIPNWPAII\":0,\"tIPNWPUII\":0,\"tIPACUNDI\":0,\"tIPNWPICI\":0,\"tIPNWPMCI\":0,\"tIPNWPDFC\":0,\"tIPNWPADI\":0," +
            "\"tIPNWPUDI\":0,\"tIPNWPICG\":0,\"tIPNWPMCG\":0,\"tIPNWPAC1\":0,\"tIPNWPAC2\":0,\"tIPNWPAC3\":0,\"tIPNWPAC4\":0,\"tIPNWPAC5\":0,\"tIPNWPAC6\":0," +
            "\"tIPNWPAC7\":0,\"tIPNWPAC8\":0,\"tIPNWPA1I\":0,\"tIPNWPA2I\":0,\"tIPNWPA3I\":0,\"tIPNWPA4I\":0,\"tIPNWPA5I\":0,\"tIPNWPA6I\":0,\"tIPNWPA7I\":0," +
            "\"tIPNWPA8I\":0,\"tIPHDICAM\":0,\"tIPHDIAMI\":0,\"tIPHDICAI\":0,\"tIPXAAUDI\":0,\"tIPXAUNDI\":0,\"tIPXPDFC\":0,\"tIPXPADI\":0,\"tIPXPUDI\":0,\"tIPXPICG\":0," +
            "\"tIPXPMCG\":0,\"tIPXPAC1\":0,\"tIPXPAC2\":0,\"tIPXPAC3\":0,\"tIPXPAC4\":0,\"tIPXPAC5\":0,\"tIPXPAC6\":0,\"tIPXPAC7\":0,\"tIPXPAC8\":0,\"tIPXHDICA\":0," +
            "\"tIPNPOFII\":0,\"tIPNPODFE\":0,\"tIPXPNUFEE\":0,\"tIPNPNUFEE\":0,\"tIPNPNUFII\":0,\"tIPRUFAII\":0,\"tIPRUFAI\":0,\"tIPNF01\":0,\"tIPUSGFEE\":0," +
            "\"tIPROBR\":0,\"tIPROACTP\":0,\"tIPROCDG\":0,\"tIPROSTEM\":0,\"tIPTKPBR1\":0,\"tIPTKPBR2\":0,\"tIPTKLBR1\":0,\"tIPTKLBR2\":0,\"tIPTKPPID\":0," +
            "\"tIPTKLPID\":0,\"tIPTKPGRP\":0,\"tIPTKPTML\":0,\"tIPTKLTID\":\"\",\"tIPPRIMBM\":0,\"tIPSECBM\":0,\"tIPEPSECB\":0,\"tIPEPPINI\":0,\"tIPPROCCD\":\"\"," +
            "\"tIPTXNTYP\":\"\",\"tIPACCTYP\":\"\",\"tIPREST\":\"\",\"tIPEPTXTY\":\"\",\"tIPEPDEB\":\"\",\"tIPEPCRE\":\"\",\"tIPISOMTI\":0,\"tIPTXNAMT\":0," +
            "\"tIPTXMODE\":\"\",\"tIPSCFUNC\":0,\"tIPSCNIMK\":0,\"tIPSCCIMK\":0,\"tIPSCOPAK\":0,\"tIPSCPPAK\":0,\"tIPSCPINK\":0,\"tIPSCPKGN\":0,\"tIPSCPK9S\":0," +
            "\"tIPGWAYFG\":\"\",\"tIPREJNO\":0,\"tIPACTCD\":0,\"tIPVSCATI\":\"\",\"tIPVSCUCD\":0,\"tIPVSEXDT\":0,\"tIPVSCAID\":\"\",\"tIPVSLARD\":0,\"tIPVSCACC\":\"\"," +
            "\"tIPVSCACT\":\"\",\"tIPVSCACN\":\"\",\"tIPVSB1AR\":0,\"tIPVSADRC\":0,\"tIPRESCVC\":0,\"tIPCVV2RC\":0,\"tIPVSEXFI\":0,\"tIPVSRET\":0,\"tIPVSSTAN\":0," +
            "\"tIPVSAUIR\":\"\",\"tIPVSTRAD\":0,\"tIPVSMETY\":0,\"tIPVSRECD\":\"\",\"tIPVSREAM\":0,\"tIPVSAICC\":0,\"tIPVSEMOD\":0,\"tIPVSPENC\":0,\"tIPVSPCON\":0," +
            "\"tIPVSPCAP\":0,\"tIPVSLAII\":0,\"tIPVSSREC\":\"\",\"tIPVSAIIC\":0,\"tIPVSNPGD\":\"\",\"tIPVSLNPG\":0,\"tIPVSLPEC\":0,\"tIPVSFIIC\":0,\"tIPVSTETY\":0," +
            "\"tIPVSTECB\":0,\"tIPVSMCRC\":\"\",\"tIPVSAMTO\":0,\"tIPVSOTDB\":0,\"tIPVSMTOR\":0,\"tIPVSAUFI\":0,\"tIPVSOTAN\":0,\"tIPVSOMTI\":0,\"tIPVSOTRD\":0," +
            "\"tIPVSOTRT\":0,\"tIPVSOAQI\":0,\"tIPVSOFOI\":0,\"tIPVSPCDN\":0,\"tIPVSCDAN\":0,\"tIPVSTRMT\":0,\"tIPVSTRMC\":0,\"tIPVSEDTI\":0,\"tIPVSECTS\":\"\"," +
            "\"tIPVSLAPS\":0,\"tIPVSB1LW\":0,\"tIPVSLCAM\":0,\"tIPVSTXCD\":0,\"tIPVSDCAF\":0,\"tIPVSPURD\":0,\"tIPVSCAPD\":0,\"tIPVSB2LW\":0,\"tIPONOTE\":0," +
            "\"tIPOBGC\":0,\"tIPNNOTE\":0,\"tIPNNBGC\":0,\"tIPOANTE\":0,\"tIPOCOINS\":0,\"tIPNANTE\":0,\"tIPNCOINS\":0,\"tIPOCHQ\":0,\"tIPNNCHQ\":0,\"tIPBCERIN(1)\":0," +
            "\"tIPBCRIN(1)\":0,\"tIPBCBRCH(1)\":0,\"tIPBCBNUO(1)\":0,\"tIPBCCT\":0,\"tIPBCEXRG\":0,\"tIPSUBSTY\":0,\"tIPGRINTD\":0,\"tIPTAXETD\":0,\"tIPAMWDTD\":0," +
            "\"tIPTINTEB\":0,\"tIPORSTDT\":0,\"tIPNSUBST\":0,\"tIPNGRINT\":0,\"tIPNTAXET\":0,\"tIPNAMWDT\":0,\"tIPNTINTE\":0,\"tIPNORSTD\":0,\"tIPSUBSTI\":0," +
            "\"tIPGRINTI\":0,\"tIPTAXETI\":0,\"tIPAMWDTI\":0,\"tIPTINTEI\":0,\"tIPORSTDI\":0,\"tIPNENFYS\":0,\"tIPNPBR\":0,\"tIPNPACTP\":0,\"tIPNPCDG\":0," +
            "\"tIPNPSTEM\":0,\"tIPNPRMDT\":0,\"tIPENFYSI\":0,\"tIPPRACCI\":0,\"tIPPRMDTI\":0,\"tIPENFYSB\":0,\"tIPEXDTFS\":0,\"tIPNWDTFS\":0,\"tIPIXBR\":0," +
            "\"tIPIXACTP\":0,\"tIPIXCDG\":0,\"tIPIXSTEM\":0,\"tIPDXRF\":\"\",\"tIPLCHQ\":0,\"tIPHCHQ\":0,\"tIPNXLLCN\":0,\"tIPLCHQII\":0,\"tIPSCQAM\":0," +
            "\"tIPSCQNII\":0,\"tIPSCQN\":0,\"tIPNXSCQN\":0,\"tIPUNCAM\":0,\"tIPCLD\":0,\"tIPRSFNO\":0,\"tIPCFRQ\":0,\"tIPPFEE\":\"\",\"tIPNCHGD\":0,\"tIPADAY\":0," +
            "\"tIPPFEEII\":0,\"tIPFEEAMT\":0,\"tIPOEODBR\":0,\"tIPNEODBR\":0,\"tIPASIOV\":0,\"tIPASINV\":0,\"tIPOSDII\":0,\"tIPPAMT\":0,\"tIPCSDEV\":0,\"tIPCSDNV\":0," +
            "\"tIPCABEV\":0,\"tIPCABNV\":0,\"tIPDBTEV\":0,\"tIPDBTNV\":0,\"tIPAMWBEV\":0,\"tIPAMWBNV\":0,\"tIPTBREE\":0,\"tIPACTPEE\":0,\"tIPCDGEE\":0,\"tIPEESTEM\":0," +
            "\"tIPICROCS\":0,\"tIPRNSCDE\":\"\",\"tIPACPRDN\":0,\"tIPACSTYP\":0,\"tIPINDCT\":0,\"tIPNEWIS1\":0,\"tIPNEWIS2\":0,\"tIPINDQL(1)\":0,\"tIPINDNO(1)\":0," +
            "\"tIPNEWIND\":\"\",\"tIPNALNAU\":0,\"tIPSECI\":0,\"tIPDTEGI\":0,\"tIPFLRDI\":0,\"tIPRVD\":0,\"tIPOLPI\":0,\"tIPTYPSEC\":0,\"tIPSCDADV\":0,\"tIPDTGNTD\":0," +
            "\"tIPFLRPDT\":0,\"tIPOLPURP\":0,\"tIPINSIND\":0,\"tIPINSII\":0,\"tIPDBL\":0,\"tIPCRL\":0,\"tIPLDBAL1\":0,\"tIPLDBAL2\":0,\"tIPDIMB1\":0,\"tIPDIMB2\":0," +
            "\"tIPDIMB3\":0,\"tIPLDB1I\":0,\"tIPLDB2I\":0,\"tIPDIMB1I\":0,\"tIPDIMB2I\":0,\"tIPDIMB3I\":0,\"tIPDBLI\":0,\"tIPCBLI\":0,\"tIPSCADI\":0,\"tIPRVDI\":0," +
            "\"tIPLAUTI\":0,\"tIPNOTIR\":0,\"tIPNOTIRV\":0,\"tIPCHGFRQ\":0,\"tIPBUSTYP\":0,\"tIPTRVARV\":0,\"tIPTARIFF\":0,\"tIPTINTTRF\":0,\"tIPTRCHGO\":0," +
            "\"tIPTCHGVI\":0,\"tIPTORVRI\":0,\"tIPTCHGVV\":0,\"tIPCQVL2I\":0,\"tIPCLDY2I\":0,\"tIPTXCVII\":0,\"tIPTORVII\":0,\"tIPTXCVVI\":0,\"tIPTORVVI\":0," +
            "\"tIPLNACI\":0,\"tIPBUSTPI\":0,\"tIPTRFVIN\":0,\"tIPINTTRI\":0,\"tIPTRCHGI\":0,\"tIPC1BR\":0,\"tIPC1ACTP\":0,\"tIPC1CDG\":0,\"tIPC1STEM\":0,\"tIPC2BR\":0," +
            "\"tIPC2ACTP\":0,\"tIPC2CDG\":0,\"tIPC2STEM\":0,\"tIPC1IND\":0,\"tIPC2IND\":0,\"tIPLIRAAI\":0,\"tIPPRDNO\":0,\"tIPSBTYP\":0,\"tIPRORJCD\":0,\"tIPRORPCD\":0," +
            "\"tIPTARIF\":0,\"tIPPRODNI\":0,\"tIPSBTYPI\":0,\"tIPTARIFI\":0,\"tIPCHGCCD\":0,\"tIPCHGCCM\":0,\"tIPRCAD\":0,\"tIPRCAMT\":0,\"tIPRCNDD\":0,\"tIPCCCDVI\":0," +
            "\"tIPCCCMVI\":0,\"tIPRCADVI\":0,\"tIPRCAMTV\":0,\"tIPRCNDDV\":0,\"tIPCCCDAY\":0,\"tIPCCCMTH\":0,\"tIPCCCDI\":0,\"tIPCCCMI\":0,\"tIPCORUVL\":0," +
            "\"tIPCORUVI\":0,\"tIPCORIVL\":0,\"tIPCORIVI\":0,\"tIPMRPAMV\":0,\"tIPAPSCVL\":0,\"tIPAPSCVI\":0,\"tIPORSDVL\":0,\"tIPORDVVL\":0,\"tIPCUSFVL\":0," +
            "\"tIPCUSFVI\":0,\"tIPMRPAMI\":0,\"tIPLIMTYP\":0,\"tIPLIMTYI\":0,\"tIPREPSRC\":0,\"tIPREPSRI\":0,\"tIPDCRAMT\":0,\"tIPDCRAMI\":0,\"tIPDECFRQ\":0," +
            "\"tIPDTNDEC\":0,\"tIPDECFRI\":0,\"tIPDTNDEI\":0,\"tIPDECACD\":0,\"tIPNODECR\":0,\"tIPREVTAM\":0,\"tIPNODECI\":0,\"tIPREVTAI\":0,\"tIPADVF01\":0," +
            "\"tIPADVF02\":0,\"tIPIBMGN\":0,\"tIPIBEXDT\":0,\"tIPIBMKCD\":0,\"tIPNBMGN\":0,\"tIPNBEXDT\":0,\"tIPNBMKCD\":0,\"tIPDBMGN\":0,\"tIPDBEXDT\":0," +
            "\"tIPDBMKCD\":0,\"tIPINFBDT\":0,\"tIPPREDBL\":0,\"tHICRTOOV\":0,\"tHISRCGOV\":0,\"tHILBL\":0,\"tHIMBL\":0,\"tHIIC\":0,\"tIPORBNTP\":0,\"tIPORRGTP\":0," +
            "\"tIPPABNTP\":0,\"tIPTMODE\":0,\"tIPRUND\":0,\"tMIADNLDI\":0,\"tIPBRBALE\":0,\"tMIEBSTXN\":0,\"tMISPFLG6\":0,\"tMITMPODP\":0,\"tMISPFLG4\":0," +
            "\"tMISPFLG3\":0,\"tMISPFLG2\":0,\"tMISPFLG1\":0,\"tMITFACLC\":0,\"tIPTRUTNO\":0,\"tIPSCERCD\":0,\"tIPINDRPD\":0,\"tIPOCTNUM\":0,\"tIPVETNUM\":0," +
            "\"tIPBAN\":0,\"tIPODSTCD\":0,\"tIPORBNDX\":0,\"tIPORRGDX\":0,\"tIPRODR\":0,\"tIPODSTBR\":0,\"tIPRODRI\":0,\"tIPOGRP\":\"\",\"tIPDBKTEL\":0," +
            "\"tIPTTAREA\":0,\"tIPTTPAGE\":0,\"tIPTTREC\":0,\"tIPITTACD\":0,\"tIPITTPNO\":0,\"tIPITTRNO\":0,\"tIPDBKMTT\":0,\"tIPTMAREA\":0,\"tIPTMPAGE\":0," +
            "\"tIPTMREC\":0,\"tIPMTTACD\":0,\"tIPMTTPNO\":0,\"tIPMTTRNO\":0,\"tIPPBNNPA\":0,\"tIPOBNNPA\":0,\"tIPPDSTCD\":0,\"tIPPDSTBR\":0,\"tIPOHOBR\":0," +
            "\"tIPORGOCD\":0,\"tIPFLORC\":0,\"tIPFLOLRS\":0,\"tIPFLOLRI\":0,\"tIPACCKST\":0,\"tIPFSOMAI\":0,\"tIPNSTOFC\":0,\"tMISEXCI\":\"\",\"tIPNIRSUB\":0," +
            "\"tIPACTPCD\":\"\",\"tIPSUBAL\":0,\"tIPTVTCPD\":0,\"tIPTNTCPD\":0,\"tIPSUPRDN\":0,\"tIPSUSBTP\":0,\"tIPSUTARI\":0,\"tIPSUINDS\":\"\",\"tIPRATMID\":\"\"," +
            "\"tIPSUTXIN\":0,\"tMIACIN3\":0,\"tMIACIN2\":0,\"tMIACIN1\":0,\"tMISGONR\":0,\"tMIKEXCR\":0,\"tMIDWNLK\":0,\"tMIPERUP\":0,\"tMITEMFL\":0,\"tIPTFLTIN\":0," +
            "\"tIPRTXNA\":0,\"tIPRTRCNO\":0,\"tIPRRCDE\":0,\"tIPMNGCHG\":0,\"tIPRATMSN\":0,\"tIPICORDT\":\"\",\"tIPTPFAMT\":0,\"tIPCBINT\":0,\"tIPICTXCD\":\"\"," +
            "\"tIPICTXNC\":\"\",\"tIPRACTI\":0,\"tIPNMLERI\":0,\"tIPICORBR\":0,\"tIPICIPTM\":\"\",\"tIPICTTNL\":0,\"tIPICTTIP\":\"\",\"tIPICTTSQ\":\"\"," +
            "\"tIPICTEL\":\"\",\"tIPICTXID\":0,\"tIPLENICD\":0,\"tIPICACTP\":0,\"tIPIWHLEN\":0,\"tIPICPDTE\":0,\"tIPICTLR\":\"\",\"tIPF03\":0,\"tIPICDMIN\":0," +
            "\"tIPICTTIN\":0,\"tIPICTMIN\":0,\"tIPICATIN\":0,\"tIPF04\":0,\"tIPF05\":0,\"tIPF06\":0,\"tIPF07\":0,\"tIPICTTYP\":0,\"tIPICCBOX\":0,\"tIPORPID\":0," +
            "\"tIPICCBKY\":0,\"tIPICTGOB\":0,\"tIPICTGOG\":0,\"tIPICOBFC\":0,\"tIPTXNCL\":0,\"tIPICF08\":0,\"tIPICTSEQ\":0,\"tIPICTDTE\":0,\"tIPICIN1\":0," +
            "\"tIPBUSIN\":0,\"tIPICIN3\":0,\"tIPRICIND\":0,\"tIPICOBIN\":0,\"tIPICRJIN\":0,\"tIPICTEF\":0,\"tIPICEXIN\":0,\"tIPRORPI\":0,\"tIPICTRPI\":0," +
            "\"tIPIACHGP\":0,\"tIPICNRSP\":0,\"tIPMPHORI\":0,\"tIPEXENVI\":0,\"tIPRSPATM\":0,\"tIPTXOPRQ\":0,\"tIPSPICT2\":0,\"tIPSPICT1\":0,\"tIPICF22\":0," +
            "\"tIPIC24TN\":0,\"tIPICTPI\":0,\"tIPICNSCD\":0,\"tMISOLIUI\":0,\"tMICUS1\":0,\"tMICUS2\":0,\"tMIJOINT\":0,\"tMIC1PLPI\":0,\"tMIC2PLPI\":0," +
            "\"tMIJNPLPI\":0,\"tMIPODFEE\":0,\"tMIPNODFE\":0,\"tMIPNAC1\":0,\"tMIPNAC2\":0,\"tMIPNAC3\":0,\"tMIPNAC4\":0,\"tMIPNAC5\":0,\"tMIPNAC6\":0," +
            "\"tMIPNAC7\":0,\"tMIPNAC8\":0,\"tMIRDACU1\":0,\"tMIRDACU2\":0,\"tMIRDAJNT\":0,\"tMIRDAPC1\":0,\"tMIRDAPC2\":0,\"tMIRDAPJT\":0,\"tMISPARE2\":0," +
            "\"tMISPARE1\":0,\"tMIPNDC\":0,\"tMIPNOIC\":0,\"tMIPNMC\":0,\"tMICRPYMT\":0,\"tMICOCRDT\":0,\"tMIPNUFEE\":0,\"tMIUSGFEE\":0,\"tMIMPRIND\":0," +
            "\"tMIMRSIND\":0,\"tMIMKYTYP\":\"\",\"tMIMKBLKS(1)\":\"\",\"tMIMKNOT(1)\":\"\",\"tMIMKCHKV(1)\":\"\",\"tMIMKMANB(1)\":\"\",\"tMIMACCNO\":\"\"," +
            "\"tMIMCISSN\":\"\",\"tMIMNKTYP\":0,\"tMIMICINC\":0,\"tMIMCRDNO\":\"\",\"tMIMCUSNO\":\"\",\"tMIMCVV\":\"\",\"tMIMPINV\":0,\"tMIMKEYID\":0," +
            "\"tMIMKMHRI\":0,\"tMIMF02\":0,\"tMIMCVFDT\":0,\"tMIMNPINB\":0,\"tMIMPVVIX\":0,\"tMIMNKYNT\":\"\",\"tMIMCSVER\":0,\"tMIMCSKCV\":\"\"," +
            "\"tMIMPSVER\":0,\"tMIMPSKCV\":\"\",\"tMIMNKYN2\":\"\",\"tMIMMAUCD\":\"\",\"tMIMPNSBL\":\"\",\"tMIMPVV\":\"\",\"tMIMF01\":\"\",\"tMIMKMHVD\":0," +
            "\"tMIMF03\":\"\",\"hLDMPRIND\":0,\"hLDMRSIND\":0,\"hLDMKYTYP\":\"\",\"hLDMKBLKS(1)\":\"\",\"hLDMKNOT(1)\":\"\",\"hLDMKCHKV(1)\":\"\",\"tMIPINDCC\":0," +
            "\"tMICSMSTF\":0,\"tMICWFFUR\":0,\"tMICWSFUR\":0,\"tMICWCFUR\":0,\"tMICWPICP\":0,\"tMICECAAI\":0,\"tMICWFFCP\":0,\"tMICACIEF\":0,\"tMICAICPF\":0," +
            "\"tMICWAAEI\":0,\"tMICWPAAB\":0,\"tMICECAAE\":0,\"tMICECAAB\":0,\"tMICCIGIP\":0,\"tMICNDPFF\":0,\"tMICNDPSF\":0,\"tMICL03\":0,\"tMICL04\":\"\"," +
            "\"tMISPRPMD\":0,\"tMIF04\":0,\"tMIOCCSUB\":0,\"tMIIIPIND\":0,\"tMIMXRPGC\":0,\"tMILDRETA\":0,\"tMIIWF02\":0,\"tMIIWF03\":0,\"tMIITMDES\":0," +
            "\"tMIACOCAP\":null,\"tMIIWSVNM\":\"\",\"tMIIWSVTP\":\"\",\"oIWSVCE\":\"\",\"tMINODEID\":\"\",\"tMIIWSTAT\":0,\"oIWSTATUS\":0,\"tMIIWSYS\":\"\"," +
            "\"tMINODE\":\"\",\"tMIIWTXID\":0,\"tMIIWVERS\":0,\"tMIIWRSPC\":0,\"tIPIWRSP\":0,\"tMILIWRAD\":0,\"tMISPMURE\":\"\",\"tMISPSP01\":null,\"tMISPSP02\":\"\"," +
            "\"tMISPSVNM\":\"\",\"tMISPACTN\":\"\",\"tMISPFROM\":\"\",\"tMISPMSID\":\"\",\"tMISPSCMR\":\"\",\"tMISPSP03\":null,\"tMISPSP04\":\"\",\"tMISPUN\":\"\"," +
            "\"tMISPUM\":\"\",\"tMISPUT\":\"\",\"tMISPUI\":\"\",\"tMISPCPMR\":\"\",\"tMISPCPTY\":\"\",\"tMISPCPID\":\"\",\"tMISPCPAI\":\"\",\"tMISPSP07\":\"\"," +
            "\"tMISPCPOT\":null,\"tMISPCPOI\":null,\"tMISPCOT\":\"\",\"tMISPAGNR\":\"\",\"tMISPAPLI\":\"\",\"tMISPSTAT\":0,\"tMISPSP05\":0,\"tMISPSP06\":0," +
            "\"tMIIPBFTP\":\"\",\"tMIOPBFTP\":\"\",\"tMIIPSBTP\":\"\",\"tMIOPSBTP\":\"\",\"tMIEDSSTA\":0,\"tMIEDF01\":0,\"tMIEDF02\":0,\"oEDIPBFTP\":\"\"," +
            "\"oEDOPBFTP\":\"\",\"oEDIPSBTP\":\"\",\"oEDOPSBTP\":\"\",\"oEDSTATUS\":0,\"oEDF03\":\"\",\"tMIPCFLAC\":0,\"tMILCONTR\":0,\"tMIOBHSGP\":0,\"tMISPBNCD\":0," +
            "\"tMISPRGCD\":0,\"tMISPRGDX\":0,\"tMILENVTR\":0,\"tMISPDCHC\":0,\"tMIF07\":0,\"tMISPRPDT\":0,\"tMIF03\":0,\"tMIF02\":0,\"tMIF01\":\"\"}";
    public static final Integer SEQNUM = 1;
    public static final Long T_IPTETIME = 153236L;
    public static final Integer T_IPPBR = 2021;
    public static final Long T_IPPSTEM = 987654321L;
    public static final Integer T_IPTTST = 123;
    public static final Integer T_IPTCLCDE = 999;
    public static final Double T_IPTAM = 100000000.00;
    public static final Integer T_IPCURCDE = 12;
    public static final Double T_HIACBL = 1234567890.99;
    public static final Long T_IPCDATE = 151013L;
    public static final Long T_IPTD = null;
    public static final String T_IPTXNARR = null;

    @Test
    public void testExecute() throws Exception {
        ParseCBSMessage parseMessage = new ParseCBSMessage();
        // given JSON string in tuple and a trident collector
        TridentTuple tuple = givenJSONTuple();
        TridentCollector collector = givenCollector();

        // when execute
        parseMessage.execute(tuple, collector);
        // then columns expected are emitted as Trident Values
        List<Object> expected = thenMessageValues();
        verify(collector).emit(expected);
    }

    @Test
    public void testParseSeqNumber() {
        ParseCBSMessage parseMessage = new ParseCBSMessage();
        // check parse sequence number
        Map<String, Object> fieldMap = parseMessage.parse(expectedJSON);
        assertThat(fieldMap.containsKey("SEQNUM"), is(true));
        assertThat(fieldMap.get("SEQNUM"), is((Object) SEQNUM));
        assertThat(fieldMap.get("SEQNUM"), instanceOf(Integer.class));
    }

    @Test
    public void testParseT_IPTETIME() {
        ParseCBSMessage parseMessage = new ParseCBSMessage();
        // check parse sequence number
        Map<String, Object> fieldMap = parseMessage.parse(expectedJSON);
        assertThat(fieldMap.containsKey("tIPTETIME"), is(true));
        assertThat(fieldMap.get("tIPTETIME"), is((Object) T_IPTETIME));
        assertThat(fieldMap.get("tIPTETIME"), instanceOf(Long.class));
    }

    @Test
    public void testParseT_IPPBR() {
        ParseCBSMessage parseMessage = new ParseCBSMessage();
        // check parse sequence number
        Map<String, Object> fieldMap = parseMessage.parse(expectedJSON);
        assertThat(fieldMap.containsKey("tIPPBR"), is(true));
        assertThat(fieldMap.get("tIPPBR"), is((Object) T_IPPBR));
        assertThat(fieldMap.get("tIPPBR"), instanceOf(Integer.class));
    }

    private TridentTuple givenJSONTuple() {
        TridentTuple mockTuple = mock(TridentTuple.class);
        when(mockTuple.getStringByField(ParseCBSMessage.FIELD_JSON_STRING)).thenReturn(expectedJSON);
        return mockTuple;
    }

    private TridentCollector givenCollector() {
        TridentCollector mockCollector = mock(TridentCollector.class);
        return mockCollector;
    }

    private List<Object> thenMessageValues() {
        Values expectedValuesFromMessage = new Values(
                SEQNUM,
                T_IPTETIME,
                T_IPPBR,
                T_IPPSTEM,
                T_IPTTST,
                T_IPTCLCDE,
                T_IPTAM,
                T_IPCURCDE,
                T_HIACBL,
                T_IPCDATE,
                T_IPTD,
                T_IPTXNARR,
                expectedJSON
        );
        return expectedValuesFromMessage;
    }


}