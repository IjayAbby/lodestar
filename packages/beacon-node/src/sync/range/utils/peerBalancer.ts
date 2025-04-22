import {CustodyConfig} from "../../../util/dataColumns.js";
import {PeerIdStr} from "../../../util/peerId.js";
import {shuffle} from "../../../util/shuffle.js";
import {sortBy} from "../../../util/sortBy.js";
import {Batch, BatchStatus} from "../batch.js";
import {ChainTarget} from "./chainTarget.js";

/**
 * Balance and organize peers to perform requests with a SyncChain
 * Shuffles peers only once on instantiation
 */
export class ChainPeersBalancer {
  private peers: PeerIdStr[];
  private columnsByPeer: Map<PeerIdStr, {custodyColumns: number[]}>;
  private targetByPeer: Map<PeerIdStr, ChainTarget>;
  private activeRequestsByPeer = new Map<PeerIdStr, number>();
  private readonly custodyConfig: CustodyConfig;

  // TODO: @matthewkeil check if this needs to be updated for custody groups
  constructor(
    peers: PeerIdStr[],
    targetByPeer: Map<PeerIdStr, ChainTarget>,
    columnsByPeer: Map<PeerIdStr, {custodyColumns: number[]}>,
    batches: Batch[],
    custodyConfig: CustodyConfig
  ) {
    this.peers = shuffle(peers);
    this.targetByPeer = targetByPeer;
    this.columnsByPeer = columnsByPeer;
    this.custodyConfig = custodyConfig;

    // Compute activeRequestsByPeer from all batches internal states
    for (const batch of batches) {
      if (batch.state.status === BatchStatus.Downloading) {
        this.activeRequestsByPeer.set(batch.state.peer, (this.activeRequestsByPeer.get(batch.state.peer) ?? 0) + 1);
      }
    }
  }

  /**
   * Return the most suitable peer to retry
   * Sort peers by (1) no failed request (2) less active requests, then pick first
   */
  bestPeerToRetryBatch(batch: Batch): PeerIdStr | undefined {
    if (batch.state.status !== BatchStatus.AwaitingDownload) {
      return;
    }
    const {partialDownload} = batch.state;

    const failedPeers = new Set(batch.getFailedPeers());
    const sortedBestPeers = sortBy(
      this.peers.filter((peerId) => {
        const pendingDataColumns = partialDownload
          ? partialDownload.pendingDataColumns
          : this.custodyConfig.sampledColumns;

        const target = this.targetByPeer.get(peerId);
        if (!target || target.slot < batch.request.startSlot) {
          return false;
        }

        const peerColumns = this.columnsByPeer.get(peerId)?.custodyColumns ?? [];
        const columns = peerColumns.reduce((acc, elem) => {
          if (pendingDataColumns.includes(elem)) {
            acc.push(elem);
          }
          return acc;
        }, [] as number[]);

        return columns.length > 0;
      }),
      (peer) => (failedPeers.has(peer) ? 1 : 0), // Sort by no failed first = 0
      (peer) => this.activeRequestsByPeer.get(peer) ?? 0 // Sort by least active req
    );
    return sortedBestPeers[0];
  }

  /**
   * Return peers with 0 or no active requests that has a higher target slot than this batch and has columns we need.
   */
  idlePeerForBatch(batch: Batch): PeerIdStr | undefined {
    const eligiblePeers: {peerId: PeerIdStr; columns: number}[] = [];
    for (const peerId of this.peers) {
      const activeRequests = this.activeRequestsByPeer.get(peerId);
      if (activeRequests != null && activeRequests > 0) {
        continue;
      }
      const target = this.targetByPeer.get(peerId);
      if (!target || target.slot < batch.request.startSlot) {
        continue;
      }

      const peerColumns = this.columnsByPeer.get(peerId)?.custodyColumns ?? [];
      const columns = peerColumns.reduce((acc, elem) => {
        if (this.custodyConfig.sampledColumns.includes(elem)) {
          acc.push(elem);
        }
        return acc;
      }, [] as number[]);

      if (columns.length > 0) {
        eligiblePeers.push({peerId, columns: columns.length});
      }
    }

    // pick idle peer that has the most columns we need
    return eligiblePeers.sort((a, b) => b.columns - a.columns)[0].peerId;
  }
}
